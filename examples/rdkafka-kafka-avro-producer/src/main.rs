use std::fs;
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
    CachedSchemaRegistryClient, SchemaReference, SchemaRegistryClient, SchemaRegistrySerializer,
    SchemaType, SubjectNameStrategy, UnregisteredSchema,
};

const TOPIC: &str = "test.avro.book";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::new("info"))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let sr = Arc::new(CachedSchemaRegistryClient::from_url(
        "http://localhost:8081",
    )?);

    // Register schemas before producing
    register_schemas(&sr).await?;

    let ser = SchemaRegistryAvroSerializer::new(sr);
    let producer = create_producer()?;

    let strategy = SubjectNameStrategy::TopicName(TOPIC);

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

        let message = FutureRecord::to(TOPIC).key(&pair.0).payload(&pair.1);

        producer
            .send(message, Timeout::Never)
            .await
            .map_err(|(e, _)| e)?;

        info!("Sent book event #{}", i);
    }

    info!("Finished sending 10 book events");

    Ok(())
}

async fn register_schemas(sr: &CachedSchemaRegistryClient) -> anyhow::Result<()> {
    // Register key schema (BookMetadata)
    let metadata_avro = fs::read_to_string("./tools/schemas/avro/book-metadata.avsc")?;
    let metadata_schema = UnregisteredSchema::schema(&metadata_avro).schema_type(SchemaType::Avro);

    sr.register_schema(&format!("{}-key", TOPIC), &metadata_schema)
        .await?;
    info!("Registered key schema: {}-key", TOPIC);

    // Register Author schema first (it's a dependency)
    let author_avro = fs::read_to_string("./tools/schemas/avro/author-value.avsc")?;
    let author_schema = UnregisteredSchema::schema(&author_avro).schema_type(SchemaType::Avro);

    sr.register_schema("test.avro.author-value", &author_schema)
        .await?;
    info!("Registered dependency schema: test.avro.author-value");

    // Register value schema (Book) with reference to Author
    let book_avro = fs::read_to_string("./tools/schemas/avro/book-value.avsc")?;
    let book_schema = UnregisteredSchema::schema(&book_avro)
        .schema_type(SchemaType::Avro)
        .references(vec![SchemaReference {
            name: String::from("com.github.schemaregistryconverter.avro.schema.Author"),
            subject: String::from("test.avro.author-value"),
            version: 1,
            references: None,
        }]);

    sr.register_schema(&format!("{}-value", TOPIC), &book_schema)
        .await?;
    info!("Registered value schema: {}-value", TOPIC);

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
