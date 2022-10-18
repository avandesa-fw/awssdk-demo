use crate::event::AuditEvent;

use aws_sdk_kinesis::model::{ShardIteratorType, StreamDescription};
use aws_sdk_kinesis::output::GetRecordsOutput;
use aws_sdk_kinesis::{Client, Config};
use color_eyre::eyre::{eyre, Result, WrapErr};

pub struct KinesisWrapper {
    stream_name: String,
    client: Client,
}

impl KinesisWrapper {
    pub fn new(stream_name: String, config: Config) -> Self {
        Self {
            stream_name,
            client: Client::from_conf(config),
        }
    }

    pub async fn describe_stream(&self) -> Result<StreamDescription> {
        self.client
            .describe_stream()
            .stream_name(&self.stream_name)
            .send()
            .await
            .wrap_err("Failed to describe stream")?
            .stream_description()
            .cloned()
            .ok_or_else(|| eyre!("No stream description"))
    }

    pub async fn get_shard_iterator(&self, shard_id: &str) -> Result<String> {
        self.client
            .get_shard_iterator()
            .stream_name(&self.stream_name)
            .shard_id(shard_id)
            .shard_iterator_type(ShardIteratorType::TrimHorizon)
            .send()
            .await
            .wrap_err("Failed to get shard iterator")?
            .shard_iterator()
            .map(|i| i.to_string())
            .ok_or_else(|| eyre!("No shard iterator"))
    }

    pub async fn get_records(&self, shard_iterator: &str) -> Result<GetRecordsOutput> {
        self.client
            .get_records()
            .shard_iterator(shard_iterator)
            .send()
            .await
            .map_err(|e| dbg!(e))
            .wrap_err("Failed to get records")
    }

    #[tracing::instrument(skip_all)]
    pub async fn read_messages_forever(&self, initial_shard_iterator: &str) -> Result<()> {
        let mut shard_iterator = initial_shard_iterator.to_string();
        loop {
            // Call GetRecords
            let resp = self.get_records(&shard_iterator).await?;
            for record in resp.records().unwrap() {
                match AuditEvent::try_from(record.data().unwrap()) {
                    Ok(AuditEvent::ProjectEvent(event)) => {
                        tracing::info!(project_id = %event.project_id, "Received project event")
                    }
                    Ok(AuditEvent::AccountEvent(event)) => {
                        tracing::info!(account_id = event.account_id, "Received account event")
                    }
                    Err(err) => {
                        let raw_message = String::from_utf8_lossy(record.data().unwrap().as_ref());
                        // CLion may display an error on this line. You can ignore it, the code is correct.
                        tracing::error!(%raw_message, "Error deserializing event: {}", err)
                    }
                }
            }

            // Update the iterator
            if let Some(next_iterator) = resp.next_shard_iterator() {
                shard_iterator = next_iterator.to_string();
            } else {
                println!("Shard ended");
                break;
            }

            // Wait 200 ms
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }

        Ok(())
    }
}
