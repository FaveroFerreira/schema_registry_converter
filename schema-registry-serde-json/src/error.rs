use std::error::Error as StdError;
use std::fmt;

use schema_registry_client::SchemaRegistryError;
use schema_registry_serde::ExtractError;

pub type BoxError = Box<dyn StdError + Send + Sync>;

#[derive(Debug)]
pub struct JsonSerializationError {
    source: BoxError,
}

impl JsonSerializationError {
    pub fn new(source: impl StdError + Send + Sync + 'static) -> Self {
        JsonSerializationError {
            source: Box::new(source),
        }
    }
}

impl From<SchemaRegistryError> for JsonSerializationError {
    fn from(error: SchemaRegistryError) -> Self {
        JsonSerializationError::new(error)
    }
}

impl From<serde_json::Error> for JsonSerializationError {
    fn from(error: serde_json::Error) -> Self {
        JsonSerializationError::new(error)
    }
}

impl StdError for JsonSerializationError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&*self.source)
    }
}

impl fmt::Display for JsonSerializationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Json serialization error: {}", self.source)
    }
}

#[derive(Debug)]
pub struct JsonDeserializationError {
    source: BoxError,
}

impl JsonDeserializationError {
    pub fn new(source: impl StdError + Send + Sync + 'static) -> Self {
        JsonDeserializationError {
            source: Box::new(source),
        }
    }
}

impl From<SchemaRegistryError> for JsonDeserializationError {
    fn from(error: SchemaRegistryError) -> Self {
        JsonDeserializationError::new(error)
    }
}

impl From<serde_json::Error> for JsonDeserializationError {
    fn from(error: serde_json::Error) -> Self {
        JsonDeserializationError::new(error)
    }
}

impl From<ExtractError> for JsonDeserializationError {
    fn from(error: ExtractError) -> Self {
        JsonDeserializationError::new(error)
    }
}

impl StdError for JsonDeserializationError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&*self.source)
    }
}

impl fmt::Display for JsonDeserializationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Json deserialization error: {}", self.source)
    }
}
