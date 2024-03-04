use std::error::Error as StdError;
use std::fmt;

use schema_registry_client::SchemaRegistryError;
use schema_registry_serde::ExtractError;

pub type BoxError = Box<dyn StdError + Send + Sync>;

#[derive(Debug)]
pub struct AvroSerializationError {
    source: BoxError,
}

impl AvroSerializationError {
    pub fn new(source: impl StdError + Send + Sync + 'static) -> Self {
        AvroSerializationError {
            source: Box::new(source),
        }
    }
}

impl From<SchemaRegistryError> for AvroSerializationError {
    fn from(error: SchemaRegistryError) -> Self {
        AvroSerializationError::new(error)
    }
}

impl From<apache_avro::Error> for AvroSerializationError {
    fn from(error: apache_avro::Error) -> Self {
        AvroSerializationError::new(error)
    }
}

impl StdError for AvroSerializationError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&*self.source)
    }
}

impl fmt::Display for AvroSerializationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Avro serialization error: {}", self.source)
    }
}

#[derive(Debug)]
pub struct AvroDeserializationError {
    source: BoxError,
}

impl AvroDeserializationError {
    pub fn new(source: impl StdError + Send + Sync + 'static) -> Self {
        AvroDeserializationError {
            source: Box::new(source),
        }
    }
}

impl From<SchemaRegistryError> for AvroDeserializationError {
    fn from(error: SchemaRegistryError) -> Self {
        AvroDeserializationError::new(error)
    }
}

impl From<apache_avro::Error> for AvroDeserializationError {
    fn from(error: apache_avro::Error) -> Self {
        AvroDeserializationError::new(error)
    }
}

impl From<ExtractError> for AvroDeserializationError {
    fn from(error: ExtractError) -> Self {
        AvroDeserializationError::new(error)
    }
}

impl StdError for AvroDeserializationError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&*self.source)
    }
}

impl fmt::Display for AvroDeserializationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Avro deserialization error: {}", self.source)
    }
}
