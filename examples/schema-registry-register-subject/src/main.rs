use std::fs;

use anyhow::Context;
use tracing::info;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

use schema_registry_converter::client::{
    CachedSchemaRegistryClient, SchemaRegistryClient, SchemaType, UnregisteredSchema,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::new("info"))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let mut subject_schema = Vec::new();

    let paths = fs::read_dir("./tools/schemas/avro").context("Failed to read directory")?;

    for path in paths {
        let path = path.unwrap().path();

        let subject = path
            .file_stem()
            .context("Failed to get file stem")?
            .to_str()
            .context("Failed to convert to string")?
            .to_string();

        let schema_str = fs::read_to_string(path)?;
        let schema = UnregisteredSchema::schema(&schema_str).schema_type(SchemaType::Avro);

        subject_schema.push((subject, schema));
    }

    let client = CachedSchemaRegistryClient::from_url("http://localhost:8081")?;

    for (subject, schema) in subject_schema {
        let schema = client.register_schema(&subject, &schema).await?;
        info!("Registered schema with id: {}", schema.id);
    }

    Ok(())
}
