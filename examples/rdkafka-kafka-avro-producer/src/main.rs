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

use schema_registry_converter::client::CachedSchemaRegistryClient;
use schema_registry_converter::serde::avro::{SchemaRegistryAvroSerializer, SubjectNameStrategy};
use schema_registry_converter::serde::SchemaRegistrySerializer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::new("info"))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let ser = create_serializer()?;
    let producer = create_producer()?;

    let topic = "example.account-created";
    let strategy = SubjectNameStrategy::TopicName(&topic);

    for i in 0..10 {
        let metadata = ExampleAccountCreatedMetadata {
            tenant: "br".to_string(),
            source: "c2c".to_string(),
        };

        let account_created = ExampleAccountCreated {
            username: "john.doe".to_string(),
            password: "12345".to_string(),
            nickname: if i % 2 == 0 {
                Some("John Doe".to_string())
            } else {
                None
            },
        };

        let key = ser.serialize_key(strategy, &metadata);
        let value = ser.serialize_value(strategy, &account_created);

        let pair = try_join(key, value).await?;

        let message = FutureRecord::to(topic).key(&pair.0).payload(&pair.1);

        producer
            .send(message, Timeout::Never)
            .await
            .map_err(|(e, _)| e)?;

        info!("Sent account created event")
    }

    Ok(())
}

#[derive(Debug, Serialize)]
struct ExampleAccountCreatedMetadata {
    tenant: String,
    source: String,
}

#[derive(Debug, Serialize)]
struct ExampleAccountCreated {
    username: String,
    password: String,
    nickname: Option<String>,
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
