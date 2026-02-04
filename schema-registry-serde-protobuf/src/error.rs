use schema_registry_client::SchemaRegistryError;
use schema_registry_serde::ExtractError;
use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum ProtoSerializationError {
    #[error(transparent)]
    SchemaRegistry(#[from] SchemaRegistryError),

    #[error("Protobuf encode error: {0}")]
    ProtobufEncode(#[from] prost::EncodeError),

    #[error("Could not resolve message index for type: {0}")]
    UnresolvableMessageType(String),

    #[error("JSON serialization error: {0}")]
    JsonSerialization(#[from] serde_json::Error),

    #[error("Dynamic message error: {0}")]
    DynamicMessage(String),

    #[error("Schema parse error: {0}")]
    SchemaParse(String),
}

#[derive(Debug, ThisError)]
pub enum ProtoDeserializationError {
    #[error(transparent)]
    SchemaRegistry(#[from] SchemaRegistryError),

    #[error("Protobuf decode error: {0}")]
    ProtobufDecode(#[from] prost::DecodeError),

    #[error("Could not resolve message name for index: {0:?}")]
    UnresolvableIndex(Vec<i32>),

    #[error("JSON deserialization error: {0}")]
    JsonDeserialization(#[from] serde_json::Error),

    #[error("Extract error: {0}")]
    Extract(#[from] ExtractError),

    #[error("Schema parse error: {0}")]
    SchemaParse(String),
}
