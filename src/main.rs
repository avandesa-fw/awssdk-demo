mod configuration;
mod dynamo;
mod logging;

use std::path::{Path, PathBuf};

use color_eyre::eyre::{Result, WrapErr};

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
    let config = configuration::load_app_config().wrap_err("Failed to load app config")?;
    let aws_config = configuration::load_aws_config(config.override_aws_endpoint)
        .await
        .wrap_err("Failed to load AWS config")?;

    let dynamo_client = dynamo::DynamoWrapper::new(config.dynamo_table_name, aws_config.dynamo);
    tracing::info!("DynamoDB Client initialized");

    dynamo_client.list_tables().await?;
    dynamo_client.send_batch_write().await?;

    Ok(())
}

pub fn load_events_file(path: &Path) -> Result<Vec<serde_json::Value>> {
    let file = std::fs::File::open(path).wrap_err("Failed to open file")?;
    serde_json::from_reader(file).wrap_err("Failed to parse JSON")
}
