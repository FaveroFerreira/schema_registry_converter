use std::error::Error as StdError;
use std::io;

use reqwest::header::{InvalidHeaderName, InvalidHeaderValue};
use thiserror::Error as ThisError;

pub type BoxError = Box<dyn StdError + Send + Sync>;

#[derive(Debug, ThisError)]
pub enum ConfigurationError {
    #[error("Error parsing header name: {source}")]
    InvalidHeaderName {
        #[from]
        source: InvalidHeaderName,
    },

    #[error("Error parsing header value: {source}")]
    InvalidHeaderValue {
        #[from]
        source: InvalidHeaderValue,
    },

    #[error("Error applying authentication header: {source}")]
    Io {
        #[from]
        source: io::Error,
    },

    #[error("Error configuring proxy: {source}")]
    Proxy {
        #[from]
        source: reqwest::Error,
    },
}

#[derive(Debug, ThisError)]
pub enum HttpCallError {
    #[error("Error parsing Schema Registry response '{body}' into '{target}': {source}")]
    JsonParse {
        body: String,
        target: &'static str,
        source: BoxError,
    },

    #[error("Upstream error: {url} returned {status}: {body}")]
    UpstreamError {
        url: String,
        status: u16,
        body: String,
    },

    #[error("Unexpected HTTP Call error: {source}")]
    Unexpected {
        #[from]
        source: reqwest::Error,
    },
}

#[derive(Debug, ThisError)]
pub enum SchemaRegistryError {
    #[error(transparent)]
    Configuration(#[from] ConfigurationError),

    #[error(transparent)]
    HttpCall(#[from] HttpCallError),

    #[error("Error parsing invalid schema type: {message}")]
    InvalidSchemaType { message: String },
}

impl SchemaRegistryError {
    pub fn invalid_schema_type<T: ToString>(s: T) -> Self {
        SchemaRegistryError::InvalidSchemaType {
            message: s.to_string(),
        }
    }
}
