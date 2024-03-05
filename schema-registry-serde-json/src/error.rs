use jsonschema::paths::PathChunk;
use jsonschema::{ErrorIterator, ValidationError};
use serde_json::Value;
use std::borrow::Cow;
use std::error::Error as StdError;
use std::fmt;

use thiserror::Error as ThisError;

use schema_registry_client::SchemaRegistryError;
use schema_registry_serde::ExtractError;

pub type BoxError = Box<dyn StdError + Send + Sync>;

#[derive(Debug, ThisError)]
pub enum JsonSerializationError {
    #[error(transparent)]
    SchemaRegistry(#[from] SchemaRegistryError),

    #[error("Error parsing schema: {0:?}")]
    SchemaValidation(Vec<SchemaValidationError>),

    #[error("Parse error: {0}")]
    Parse(#[from] serde_json::Error),

    #[error(transparent)]
    Other(#[from] BoxError),
}

#[derive(Debug, ThisError)]
#[error("Expected {expected} at {at}, found {received}")]
pub struct SchemaValidationError {
    received: Cow<'static, str>,
    expected: Cow<'static, str>,
    at: Cow<'static, str>,
}

impl From<ValidationError<'_>> for JsonSerializationError {
    fn from(errors: ValidationError<'_>) -> Self {
        let errors = vec![SchemaValidationError::from(errors)];
        JsonSerializationError::SchemaValidation(errors)
    }
}

impl From<ErrorIterator<'_>> for JsonSerializationError {
    fn from(errors: ErrorIterator<'_>) -> Self {
        let errors = errors.map(SchemaValidationError::from).collect();
        JsonSerializationError::SchemaValidation(errors)
    }
}

impl From<ValidationError<'_>> for SchemaValidationError {
    fn from(error: ValidationError<'_>) -> Self {
        let actual_type = match *error.instance {
            Value::Null => "null",
            Value::Bool(_) => "boolean",
            Value::Number(_) => "number",
            Value::String(_) => "string",
            Value::Array(_) => "array",
            Value::Object(_) => "object",
        };
        let expected_type = format!("{:?}", error.kind);
        let path = error
            .schema_path
            .iter()
            .map(|s| match s {
                PathChunk::Property(p) => p.to_string(),
                PathChunk::Index(i) => i.to_string(),
                PathChunk::Keyword(k) => k.to_string(),
            })
            .collect::<Vec<_>>()
            .join(".");

        SchemaValidationError {
            received: actual_type.into(),
            expected: expected_type.into(),
            at: path.into(),
        }
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
