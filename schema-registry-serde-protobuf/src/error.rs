use schema_registry_client::SchemaRegistryError;
use thiserror::Error as ThisError;

use schema_registry_serde::ExtractError;

#[derive(Debug, ThisError)]
pub enum ProtoDeserializationError {
    #[error(transparent)]
    SchemaRegistry(#[from] SchemaRegistryError),

    #[error("Error extracting schema id and payload from message bytes: {0}")]
    Extract(#[from] ExtractError),
}

#[derive(Debug, ThisError)]
pub enum ProtoSerializationError {
    #[error(transparent)]
    SchemaRegistry(#[from] SchemaRegistryError),
}
