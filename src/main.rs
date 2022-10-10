use aws_sdk_dynamodb::model::AttributeValue;
use aws_sdk_dynamodb::{Client, Endpoint};
use aws_types::region::Region;
use chrono::Utc;
use color_eyre::eyre::{eyre, Result, WrapErr};
use http::Uri;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();
    // Pretty error printing
    color_eyre::install()?;

    // Load config
    dotenv::dotenv().wrap_err("Failed to load environment")?;
    tracing::info!("Loading AWS configuration");
    let aws_config = aws_config::from_env()
        .region(Region::new("us-east-1"))
        .endpoint_resolver(Endpoint::immutable(Uri::from_static(
            "http://localhost:4566",
        )))
        .load()
        .await;
    let dynamodb_config = aws_sdk_dynamodb::config::Builder::from(&aws_config).build();
    let client = Client::from_conf(dynamodb_config);
    tracing::info!("DynamoDB Client initialized");

    list_tables(&client).await?;
    send_sample_item(&client).await?;

    Ok(())
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

#[tracing::instrument(skip(client))]
pub async fn send_sample_item(client: &Client) -> Result<()> {
    let project_id = Uuid::new_v4();
    let now = format!("{:.3}", (Utc::now().timestamp_millis() as f64) / 1000_f64);

    client
        .put_item()
        .table_name("AuditLog")
        .item("ProjectId", AttributeValue::S(project_id.to_string()))
        .item("EventTimestamp", AttributeValue::N(now))
        .send()
        .await
        .wrap_err("Failed to put item")?;

    Ok(())
}
