use std::sync::Arc;

use futures::future::try_join;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;
use serde::Serialize;
use tracing::info;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

use schema_registry_converter::avro::SchemaRegistryAvroSerializer;
use schema_registry_converter::{
    CachedSchemaRegistryClient, SchemaRegistrySerializer, SubjectNameStrategy,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::new("info"))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let ser = create_serializer()?;
    let producer = create_producer()?;

    let topic = "test.avro.book";
    let strategy = SubjectNameStrategy::TopicName(&topic);

    for i in 0..10 {
        let metadata = BookMetadata {
            language: Language::EnUs,
        };

        let author = Author {
            id: 1,
            name: "Franz Kafka".to_string(),
            email: None,
        };

        let book = Book {
            id: i,
            title: "The Trial".to_string(),
            author,
        };

        let key = ser.serialize_key(strategy, &metadata);
        let value = ser.serialize_value(strategy, &book);

        let pair = try_join(key, value).await?;

        let message = FutureRecord::to(topic).key(&pair.0).payload(&pair.1);

        producer
            .send(message, Timeout::Never)
            .await
            .map_err(|(e, _)| e)?;

        info!("Sent book event")
    }

    Ok(())
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Language {
    PtBr,
    EnUs,
    EsEs,
}

#[derive(Debug, Serialize)]
pub struct BookMetadata {
    pub language: Language,
}

#[derive(Debug, Serialize)]
pub struct Book {
    pub id: i32,
    pub title: String,
    pub author: Author,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Author {
    pub id: i32,
    pub name: String,
    pub email: Option<String>,
}

fn create_producer() -> anyhow::Result<FutureProducer> {
    let producer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create::<FutureProducer>()?;

    Ok(producer)
}

fn create_serializer() -> anyhow::Result<SchemaRegistryAvroSerializer> {
    let sr = Arc::new(CachedSchemaRegistryClient::from_url(
        "http://localhost:8081",
    )?);

    Ok(SchemaRegistryAvroSerializer::new(sr))
}
