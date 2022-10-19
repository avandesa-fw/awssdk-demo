use aws_sdk_kinesis::types::Blob;
use serde::Deserialize;
use thiserror::Error;
use uuid::Uuid;

type JsonObject = serde_json::Map<String, serde_json::Value>;

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum Event {
    Project(ProjectEvent),
    Account(AccountEvent),
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct ProjectEvent {
    pub project_id: Uuid,
    pub entity_type: String,
    pub entity_id: String,
    #[serde(flatten)]
    pub event_data: JsonObject,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct AccountEvent {
    pub account_id: u64,
    #[serde(flatten)]
    pub event_data: JsonObject,
}

#[derive(Debug, Error)]
#[error("Failed to deserialize Event: {0}")]
pub struct EventDeserializeError(#[from] serde_json::Error);

impl TryFrom<&Blob> for Event {
    type Error = EventDeserializeError;

    fn try_from(blob: &Blob) -> Result<Self, Self::Error> {
        serde_json::from_slice(blob.as_ref()).map_err(Into::into)
    }
}

#[cfg(test)]
mod test {
    use super::{AccountEvent, Event, ProjectEvent};

    use aws_sdk_kinesis::types::Blob;
    use rand::Rng;
    use serde_json::{json, Value};
    use uuid::Uuid;

    /// Helper to create a `Blob` from a `serde_json::Value`
    fn blob_from_json(val: Value) -> Blob {
        Blob::new(serde_json::to_vec(&val).unwrap())
    }

    #[test]
    fn deserializes_project_events() {
        let sample_project_id = Uuid::new_v4();
        let sample_entity_id = Uuid::new_v4();
        let blob = blob_from_json(json!({
            "ProjectId": sample_project_id.to_string(),
            "EntityType": "task",
            "EntityId": sample_entity_id.to_string(),
        }));

        let deserialized = Event::try_from(&blob).expect("successful deserialization");
        assert_eq!(
            deserialized,
            Event::Project(ProjectEvent {
                project_id: sample_project_id,
                entity_type: "task".to_string(),
                entity_id: sample_entity_id.to_string(),
                event_data: serde_json::Map::new(),
            })
        );
    }

    #[test]
    fn deserializes_account_events() {
        let sample_account_id = rand::thread_rng().gen::<u64>();
        let blob = blob_from_json(json!({
            "AccountId": sample_account_id,
        }));

        let deserialized = Event::try_from(&blob).expect("successful deserialization");
        assert_eq!(
            deserialized,
            Event::Account(AccountEvent {
                account_id: sample_account_id,
                event_data: serde_json::Map::new(),
            })
        );
    }

    #[test]
    fn puts_extra_fields_in_map() {
        let sample_account_id = rand::thread_rng().gen::<u64>();
        let blob = blob_from_json(json!({
            "AccountId": sample_account_id,
            "Foo": "bar",
            "Baz": { "InnerField": 200 },
        }));

        let deserialized = Event::try_from(&blob).expect("successful deserialization");
        let expected_map = json!({
            "Foo": "bar",
            "Baz": { "InnerField": 200 },
        })
        .as_object()
        .cloned()
        .unwrap();
        assert_eq!(
            deserialized,
            Event::Account(AccountEvent {
                account_id: sample_account_id,
                event_data: expected_map,
            })
        );
    }

    #[test]
    fn fails_to_deserialize_invalid_event() {
        let blob = blob_from_json(json!({}));
        let result = Event::try_from(&blob);
        claims::assert_err!(result);
    }

    #[test]
    fn it_works() {
        assert!(true)
    }
}
