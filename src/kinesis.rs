use crate::event::Event;

use aws_sdk_kinesis::model::{Record, ShardIteratorType, StreamDescription};
use aws_sdk_kinesis::output::GetRecordsOutput;
use aws_sdk_kinesis::{Client, Config};
use color_eyre::eyre::{eyre, Result, WrapErr};
use tokio::sync::mpsc::UnboundedSender;

#[derive(Clone)]
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
            .wrap_err("Failed to call `DescribeStream`")?
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
            .wrap_err("Failed to call `GetShardIterator`")?
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
            .wrap_err("Failed to call `GetRecords`")
    }
}

pub struct KinesisShardReader {
    shard_id: String,
    client: KinesisWrapper,
    event_tx: UnboundedSender<Event>,
}

impl KinesisShardReader {
    pub fn new(shard_id: String, client: KinesisWrapper, event_tx: UnboundedSender<Event>) -> Self {
        Self {
            shard_id,
            client,
            event_tx,
        }
    }

    #[tracing::instrument(skip_all)]
    pub async fn run_until_shard_closed(self) -> Result<()> {
        // Obtain an initial shard iterator
        let mut shard_iterator = self
            .client
            .get_shard_iterator(&self.shard_id)
            .await
            .wrap_err("Failed to obtain shard iterator")?;

        loop {
            // Call GetRecords
            let resp = self.client.get_records(&shard_iterator).await?;

            // Handle each record received
            for record in resp.records().unwrap() {
                self.handle_record(record).await?;
            }

            // Update the iterator
            if let Some(next_iterator) = resp.next_shard_iterator() {
                shard_iterator = next_iterator.to_string();
            } else {
                println!("Shard ended");
                break;
            }

            // Wait 200 ms (so as to not exceed the 5 transactions/second limit
            // TODO: make this a config option
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }

        Ok(())
    }

    #[tracing::instrument(
        skip(self, record),
        fields(shard_id = self.shard_id, record_sequence_number = record.sequence_number()),
    )]
    async fn handle_record(&self, record: &Record) -> Result<()> {
        tracing::debug!("Handling record");
        match Event::try_from(record.data().unwrap()) {
            Ok(event) => {
                match &event {
                    Event::Project(project_event) => {
                        tracing::debug!(project_id = %project_event.project_id, "Received project event")
                    }
                    Event::Account(account_event) => {
                        tracing::debug!(
                            account_id = account_event.account_id,
                            "Received account event"
                        );
                    }
                }
                // Send the parsed event over the channel for someone else to handle
                self.event_tx
                    .send(event)
                    .wrap_err("Failed to send event over channel")?;
            }
            // Don't pass the error up. All we have is a malformed message, report it and move on
            Err(err) => {
                let raw_message = String::from_utf8_lossy(record.data().unwrap().as_ref());
                // CLion may display an error on this line. You can ignore it, the code is correct.
                tracing::error!(%raw_message, "{}", err);
            }
        }

        Ok(())
    }
}
