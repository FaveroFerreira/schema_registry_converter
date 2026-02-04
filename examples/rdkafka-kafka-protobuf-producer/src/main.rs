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

use schema_registry_converter::protobuf::SchemaRegistryProtoSerializer;
use schema_registry_converter::{
    CachedSchemaRegistryClient, SchemaReference, SchemaRegistryClient, SchemaRegistrySerializer,
    SchemaType, SubjectNameStrategy, UnregisteredSchema,
};

const TOPIC: &str = "test.protobuf.book";

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

    let ser = SchemaRegistryProtoSerializer::new(sr);
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
    let metadata_proto = fs::read_to_string("./tools/schemas/protobuf/book-metadata.proto")?;
    let metadata_schema =
        UnregisteredSchema::schema(&metadata_proto).schema_type(SchemaType::Protobuf);

    sr.register_schema(&format!("{}-key", TOPIC), &metadata_schema)
        .await?;
    info!("Registered key schema: {}-key", TOPIC);

    // Register Author schema first (it's a dependency)
    let author_proto = fs::read_to_string("./tools/schemas/protobuf/author-value.proto")?;
    let author_schema = UnregisteredSchema::schema(&author_proto).schema_type(SchemaType::Protobuf);

    sr.register_schema("test.protobuf.author-value", &author_schema)
        .await?;
    info!("Registered dependency schema: test.protobuf.author-value");

    // Register value schema (Book) with reference to Author
    let book_proto = fs::read_to_string("./tools/schemas/protobuf/book-value.proto")?;
    let book_schema = UnregisteredSchema::schema(&book_proto)
        .schema_type(SchemaType::Protobuf)
        .references(vec![SchemaReference {
            name: String::from("author-value.proto"),
            subject: String::from("test.protobuf.author-value"),
            version: 1,
            references: None,
        }]);

    sr.register_schema(&format!("{}-value", TOPIC), &book_schema)
        .await?;
    info!("Registered value schema: {}-value", TOPIC);

    Ok(())
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
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
    pub id: i64,
    pub title: String,
    pub author: Author,
}

#[derive(Debug, Serialize)]
pub struct Author {
    pub id: i64,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
}

fn create_producer() -> anyhow::Result<FutureProducer> {
    let producer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create::<FutureProducer>()?;

    Ok(producer)
}
