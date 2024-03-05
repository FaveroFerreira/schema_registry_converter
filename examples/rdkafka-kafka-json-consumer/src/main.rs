use std::sync::Arc;

use futures::future::try_join;
use futures::StreamExt;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};
use serde::Deserialize;
use tracing::{error, info, instrument};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

use schema_registry_converter::json::SchemaRegistryJsonDeserializer;
use schema_registry_converter::{CachedSchemaRegistryClient, SchemaRegistryDeserializer};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::new("info"))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let de = crate_deserializer()?;

    let consumer = crate_consumer()?;
    consumer.subscribe(&["json.account-created"])?;

    let mut stream = consumer.stream();

    while let Some(Ok(message)) = stream.next().await {
        let key = de.deserialize(message.key());
        let value = de.deserialize(message.payload());

        match try_join(key, value).await {
            Ok(pair) => {
                on_account_created(pair);
            }
            Err(e) => {
                error!("Failed to deserialize message: {:?}", e);
            }
        }

        consumer.commit_message(&message, CommitMode::Async)?;
    }

    Ok(())
}

#[instrument(name = "on_account_created", skip(pair))]
fn on_account_created(pair: (ExampleAccountCreatedMetadata, ExampleAccountCreated)) {
    info!("Received account created event");

    info!("Metadata: {:?}", pair.0);
    info!("Value: {:?}", pair.1);
}

#[derive(Debug, Deserialize)]
struct ExampleAccountCreatedMetadata {
    tenant: String,
    source: String,
}

#[derive(Debug, Deserialize)]
struct ExampleAccountCreated {
    username: String,
    password: String,
    nickname: Option<String>,
}

fn crate_consumer() -> anyhow::Result<StreamConsumer> {
    let consumer = ClientConfig::new()
        .set("group.id", "example-rdkafka-kafka-json-consumer")
        .set("bootstrap.servers", "localhost:9092")
        .create::<StreamConsumer>()?;

    Ok(consumer)
}

fn crate_deserializer() -> anyhow::Result<SchemaRegistryJsonDeserializer> {
    let sr = Arc::new(CachedSchemaRegistryClient::from_url(
        "http://localhost:8081",
    )?);

    Ok(SchemaRegistryJsonDeserializer::new(sr))
}
