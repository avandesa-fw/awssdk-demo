use aws_sdk_kinesis::types::Blob;
use serde::Deserialize;
use thiserror::Error;
use uuid::Uuid;

type JsonObject = serde_json::Map<String, serde_json::Value>;

#[derive(Debug)]
pub enum AuditEvent {
    ProjectEvent(ProjectEvent),
    AccountEvent(AccountEvent),
}

#[derive(Debug, Deserialize)]
pub struct ProjectEvent {
    #[serde(rename = "ProjectId")]
    pub project_id: Uuid,
    #[serde(rename = "EntityType")]
    pub entity_type: String,
    #[serde(rename = "EntityId")]
    pub entity_id: String,
    #[serde(flatten)]
    pub event_data: JsonObject,
}

#[derive(Debug, Deserialize)]
pub struct AccountEvent {
    #[serde(rename = "AccountId")]
    pub account_id: u64,
    #[serde(flatten)]
    pub event_data: JsonObject,
}

#[derive(Debug, Error)]
pub enum AuditEventDeserializeError {
    #[error("Either AccountId or ProjectId must be present")]
    MissingAccountOrProjectId,
    #[error("JSON value must be an Object")]
    InvalidJsonValue,
    #[error("Kinesis blob is not valid JSON: {0}")]
    MalformedJson(#[source] serde_json::Error),
    #[error("JSON value is not a valid ProjectEvent: {0}")]
    InvalidProjectEvent(#[source] serde_json::Error),
    #[error("JSON value is not a valid AccountEvent: {0}")]
    InvalidAccountEvent(#[source] serde_json::Error),
}

impl TryFrom<&Blob> for AuditEvent {
    type Error = AuditEventDeserializeError;

    fn try_from(blob: &Blob) -> Result<Self, Self::Error> {
        let json = serde_json::from_slice::<serde_json::Value>(blob.as_ref())
            .map_err(AuditEventDeserializeError::MalformedJson)?;

        let object = json
            .as_object()
            .ok_or(AuditEventDeserializeError::InvalidJsonValue)?;

        if object.contains_key("ProjectId") {
            tracing::debug!("Deserializing event as a ProjectEvent");
            serde_json::from_value(json)
                .map(AuditEvent::ProjectEvent)
                .map_err(AuditEventDeserializeError::InvalidProjectEvent)
        } else if object.contains_key("AccountId") {
            tracing::debug!("Deserializing event as an AccountEvent");
            serde_json::from_value(json)
                .map(AuditEvent::AccountEvent)
                .map_err(AuditEventDeserializeError::InvalidAccountEvent)
        } else {
            Err(AuditEventDeserializeError::MissingAccountOrProjectId)
        }
    }
}
