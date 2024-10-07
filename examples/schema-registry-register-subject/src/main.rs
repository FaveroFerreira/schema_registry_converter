use std::fs;

use tracing::info;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

use schema_registry_converter::{
    CachedSchemaRegistryClient, SchemaReference, SchemaRegistryClient, SchemaType,
    UnregisteredSchema, Version,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::new("info"))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let sr = CachedSchemaRegistryClient::from_url("http://localhost:8081")?;

    avro_key(&sr).await?;
    avro_value(&sr).await?;

    Ok(())
}

async fn avro_key(sr: &CachedSchemaRegistryClient) -> anyhow::Result<()> {
    let book_metadata = fs::read_to_string("./tools/schemas/avro/book-metadata.avsc")?;
    let metadata = UnregisteredSchema::schema(&book_metadata).schema_type(SchemaType::Avro);

    let _ = sr.register_schema("test.avro.book-key", &metadata).await?;

    let schema = sr
        .get_schema_by_subject("test.avro.book-key", Version::Latest)
        .await?;

    info!("Book Key: {:?}", schema);

    Ok(())
}

#[tracing::instrument(skip(sr))]
async fn avro_value(sr: &CachedSchemaRegistryClient) -> anyhow::Result<()> {
    let author_content = fs::read_to_string("./tools/schemas/avro/author-value.avsc")?;
    let author = UnregisteredSchema::schema(&author_content).schema_type(SchemaType::Avro);

    let _ = sr
        .register_schema("test.avro.author-value", &author)
        .await?;

    let book_content = fs::read_to_string("./tools/schemas/avro/book-value.avsc")?;
    let book = UnregisteredSchema::schema(&book_content)
        .schema_type(SchemaType::Avro)
        .references(vec![SchemaReference {
            name: String::from("Author"),
            subject: String::from("test.avro.author-value"),
            version: 1,
            references: None,
        }]);

    let _ = sr.register_schema("test.avro.book-value", &book).await?;

    let schema = sr
        .get_schema_by_subject("test.avro.book-value", Version::Latest)
        .await?;

    info!("Book Value: {:?}", schema);

    Ok(())
}
