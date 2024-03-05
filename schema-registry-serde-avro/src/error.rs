use thiserror::Error as ThisError;

use schema_registry_client::SchemaRegistryError;
use schema_registry_serde::ExtractError;

#[derive(Debug, ThisError)]
pub enum AvroSerializationError {
    #[error(transparent)]
    SchemaRegistry(#[from] SchemaRegistryError),

    #[error("Avro error: {0}")]
    Avro(#[from] apache_avro::Error),
}

#[derive(Debug, ThisError)]
pub enum AvroDeserializationError {
    #[error(transparent)]
    SchemaRegistry(#[from] SchemaRegistryError),

    #[error("Avro error: {0}")]
    Avro(#[from] apache_avro::Error),

    #[error("Error extracting schema id and payload from message bytes: {0}")]
    Extract(#[from] ExtractError),
}
