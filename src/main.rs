mod configuration;
mod dynamo;
mod event;
mod kinesis;
mod logging;

use configuration::{AwsConfig, BridgeServiceConfig};

use std::path::{Path, PathBuf};

use crate::event::Event;
use crate::kinesis::KinesisShardReader;
use color_eyre::eyre::{Result, WrapErr};
use tokio::sync::mpsc::UnboundedReceiver;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    logging::init_tracing();

    // Pretty error printing
    color_eyre::install()?;

    let events_file_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "events.json".to_string());
    let events_file_path = PathBuf::from(events_file_path);
    let _events = load_events_file(&events_file_path).wrap_err("Failed to load events file")?;

    // Load config
    let config = BridgeServiceConfig::load().wrap_err("Failed to load app config")?;
    let aws_config = AwsConfig::load(config.override_aws_endpoint)
        .await
        .wrap_err("Failed to load AWS config")?;

    // Initialize dynamo and kinesis clients
    let dynamo_client = dynamo::DynamoWrapper::new(config.dynamo_table_name, aws_config.dynamo);
    tracing::info!("DynamoDB Client initialized");

    let kinesis_client =
        kinesis::KinesisWrapper::new(config.kinesis_stream_name, aws_config.kinesis);
    tracing::info!("Kinesis Client initialized");

    // Sample calls for dynamo
    dynamo_client.list_tables().await?;
    dynamo_client.send_batch_write().await?;

    // Get metadata on kinesis stream
    let stream = kinesis_client
        .describe_stream()
        .await
        .wrap_err("Failed to describe stream")?;
    let first_shard_id = stream.shards().expect("at least one shard")[0]
        .shard_id()
        .expect("shard id");

    // Create a channel over which kinesis shard readers can send events
    let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();
    let shard_reader = KinesisShardReader::new(
        first_shard_id.to_string(),
        // The shard reader gets a copy of the `Sender`
        kinesis_client.clone(),
        event_tx.clone(),
    );

    // Start the shard reader in an asynchronous task
    let shard_reader_handle = tokio::task::spawn(shard_reader.run_until_shard_closed());

    // Start another task for the receiver of the events (eventually this would be something that
    // writes events to DynamoDB or processes them for webhooks
    let shard_collator_thread = tokio::task::spawn(receive_events(event_rx));

    // Wait for the tasks to complete
    shard_reader_handle
        .await
        .unwrap()
        .wrap_err("Error in shard reader")?;
    shard_collator_thread.await.unwrap();
    Ok(())
}

async fn receive_events(mut event_rx: UnboundedReceiver<Event>) {
    while let Some(event) = event_rx.recv().await {
        dbg!(&event);
    }
}

pub fn load_events_file(path: &Path) -> Result<Vec<serde_json::Value>> {
    let file = std::fs::File::open(path).wrap_err("Failed to open file")?;
    serde_json::from_reader(file).wrap_err("Failed to parse JSON")
}
