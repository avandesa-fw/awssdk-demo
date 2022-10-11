mod batch_write;
mod configuration;
mod logging;

use std::path::{Path, PathBuf};

use aws_sdk_dynamodb::Client;
use color_eyre::eyre::{eyre, Result, WrapErr};

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
    let (_, dynamo_config) = configuration::load_aws_config(config.override_aws_endpoint)
        .await
        .wrap_err("Failed to load AWS config")?;

    let dynamo_client = Client::from_conf(dynamo_config);
    tracing::info!("DynamoDB Client initialized");

    list_tables(&dynamo_client).await?;
    batch_write::send_batch_write(&dynamo_client).await?;

    Ok(())
}

pub fn load_events_file(path: &Path) -> Result<Vec<serde_json::Value>> {
    let file = std::fs::File::open(path).wrap_err("Failed to open file")?;
    serde_json::from_reader(file).wrap_err("Failed to parse JSON")
}

#[tracing::instrument(skip(client))]
pub async fn list_tables(client: &Client) -> Result<()> {
    let tables = client
        .list_tables()
        .send()
        .await
        .wrap_err("Failed to list tables")?;
    let table_names = tables
        .table_names()
        .ok_or_else(|| eyre!("No tables present"))?;

    println!("Tables:");
    for table in table_names {
        println!("\t{}", table);
    }

    Ok(())
}
