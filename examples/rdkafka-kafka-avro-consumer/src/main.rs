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

use schema_registry_converter::avro::SchemaRegistryAvroDeserializer;
use schema_registry_converter::CachedSchemaRegistryClient;
use schema_registry_converter::SchemaRegistryDeserializer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::new("info"))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let de = create_deserializer()?;

    let consumer = create_consumer()?;
    consumer.subscribe(&["test.avro.book"])?;

    let mut stream = consumer.stream();

    while let Some(Ok(message)) = stream.next().await {
        let key = de.deserialize(message.key());
        let value = de.deserialize(message.payload());

        match try_join(key, value).await {
            Ok(pair) => {
                handle_message(pair);
            }
            Err(e) => {
                error!("Failed to deserialize message: {:?}", e);
            }
        }

        consumer.commit_message(&message, CommitMode::Async)?;
    }

    Ok(())
}

#[instrument(name = "on_book_event", skip(pair))]
fn handle_message(pair: (BookMetadata, Book)) {
    info!("Received book event");

    info!("Metadata: {:?}", pair.0);
    info!("Value: {:?}", pair.1);
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Language {
    PtBr,
    EnUs,
    EsEs,
}

#[derive(Debug, Deserialize)]
pub struct BookMetadata {
    pub language: Language,
}

#[derive(Debug, Deserialize)]
pub struct Book {
    pub id: i32,
    pub title: String,
    pub author: Author,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Author {
    pub id: i32,
    pub name: String,
    pub email: Option<String>,
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
