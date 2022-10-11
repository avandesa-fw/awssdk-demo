use aws_sdk_dynamodb::model::{AttributeValue, PutRequest, WriteRequest};
use aws_sdk_dynamodb::Client;
use chrono::Utc;
use color_eyre::eyre::{Result, WrapErr};
use serde_json::{Map, Value};
use uuid::Uuid;

fn json_to_attribute(value: Value) -> AttributeValue {
    match value {
        Value::Number(num) => AttributeValue::N(num.to_string()),
        Value::String(string) => AttributeValue::S(string),
        Value::Null => todo!(),
        Value::Bool(_) => todo!(),
        Value::Array(_) => todo!(),
        Value::Object(_) => todo!(),
    }
}

fn write_request_from_json_object(
    project_id: Uuid,
    event_timestamp: i64,
    value: Map<String, Value>,
) -> WriteRequest {
    // Build a hash map of AttributeValues
    let mut map = value
        .into_iter()
        .map(|(key, value)| (key, json_to_attribute(value)))
        .collect::<std::collections::HashMap<_, _>>();
    // Add the required key fields
    map.insert(
        "ProjectId".to_string(),
        AttributeValue::S(project_id.to_string()),
    );
    map.insert(
        "EventTimestamp".to_string(),
        AttributeValue::N(event_timestamp.to_string()),
    );

    // Make a PutRequest into a WriteRequest
    let put_request = PutRequest::builder().set_item(Some(map)).build();
    WriteRequest::builder().put_request(put_request).build()
}

fn random_event() -> (Uuid, i64, Map<String, Value>) {
    (
        Uuid::new_v4(),
        Utc::now().timestamp_millis(),
        serde_json::json!({
            "SomeKey": "a string value",
            "Another key": 123456,
        })
        .as_object()
        .cloned()
        .unwrap(),
    )
}

#[tracing::instrument(skip(client))]
pub async fn send_batch_write(client: &Client) -> Result<()> {
    let write_requests = std::iter::repeat_with(random_event)
        .take(10)
        .map(|(project_id, event_timestamp, value)| {
            write_request_from_json_object(project_id, event_timestamp, value)
        })
        .collect::<Vec<_>>();

    // Make BatchWriteItem, set `request_items` with table name as key
    client
        .batch_write_item()
        .request_items("AuditLog", write_requests)
        .send()
        .await
        .wrap_err("Failed to batch write")?;

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
