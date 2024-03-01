use std::io;

use reqwest::header::{InvalidHeaderName, InvalidHeaderValue};
use serde_json::Value as JsonValue;
use thiserror::Error as ThisError;

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

    #[error("Error formatting auth header: {source}")]
    Io {
        #[from]
        source: io::Error,
    },

    #[error("Invalid proxy configuration: {source}")]
    Proxy {
        #[from]
        source: reqwest::Error,
    },
}

#[derive(Debug, ThisError)]
pub enum HttpCallError {
    #[error("Error parsing Schema REgistry response '{response}': {source}")]
    JsonParse {
        response: JsonValue,
        source: reqwest::Error,
    },

    #[error("HTTP call error: {source}")]
    Generic {
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
}
