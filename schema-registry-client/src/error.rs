use std::io;

use reqwest::header::{InvalidHeaderName, InvalidHeaderValue};
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
pub enum SchemaRegistryError {
    #[error(transparent)]
    Configuration(#[from] ConfigurationError),

    #[error("Http client error: {source}")]
    HttpClient {
        #[from]
        source: reqwest::Error,
    },
}
