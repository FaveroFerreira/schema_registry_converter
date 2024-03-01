use std::sync::Arc;

use futures::StreamExt;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};
use serde::Deserialize;
use tracing::{error, info, instrument};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

use schema_registry_converter::client::CachedSchemaRegistryClient;
use schema_registry_converter::serde::avro::SchemaRegistryAvroDeserializer;
use schema_registry_converter::serde::SchemaRegistryDeserializer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::new("info"))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let deserializer = create_deserializer()?;

    let consumer = create_consumer()?;
    consumer.subscribe(&["example.account-created"])?;

    let mut stream = consumer.stream();

    while let Some(Ok(message)) = stream.next().await {
        match deserializer.deserialize(message.payload()).await {
            Ok(account_created) => on_account_created(account_created),
            Err(e) => error!("Failed to deserialize message: {}", e),
        }

        consumer.commit_message(&message, CommitMode::Async)?;
    }

    Ok(())
}

#[instrument(
    name = "on_account_created",
    skip(account_created),
    fields(
        username = %account_created.username,
        password = %account_created.password,
        nickname = ?account_created.nickname
    )
)]
fn on_account_created(account_created: ExampleAccountCreated) {
    info!("Received account created event");
}

#[derive(Debug, Deserialize)]
struct ExampleAccountCreated {
    username: String,
    password: String,
    nickname: Option<String>,
}

fn create_consumer() -> anyhow::Result<StreamConsumer> {
    let consumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "example-rdkafka-kafka-avro-consumer")
        .set("auto.offset.reset", "beginning")
        .create::<StreamConsumer>()?;

    Ok(consumer)
}

fn create_deserializer() -> anyhow::Result<SchemaRegistryAvroDeserializer> {
    let sr = Arc::new(CachedSchemaRegistryClient::from_url(
        "http://localhost:8081",
    )?);

    Ok(SchemaRegistryAvroDeserializer::new(sr))
}
