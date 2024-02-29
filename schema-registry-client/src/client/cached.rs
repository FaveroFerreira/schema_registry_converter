use crate::client::{util, SchemaRegistryClient};
use crate::config::SchemaRegistryConfig;
use crate::error::{ConfigurationError, SchemaRegistryError};
use reqwest::header::HeaderMap;
use reqwest::Client;
use std::sync::Arc;

pub struct CachedSchemaRegistryClient {
    pub http: Client,
    pub urls: Arc<[String]>,
}

impl CachedSchemaRegistryClient {
    pub fn from_conf(conf: SchemaRegistryConfig) -> Result<Self, SchemaRegistryError> {
        let urls = Arc::from(conf.urls);
        let http = util::build_http_client(&conf)?;

        Ok(Self { http, urls })
    }
}

impl SchemaRegistryClient for CachedSchemaRegistryClient {
    async fn get_schema_by_id(&self, id: u32) -> Result<(), ()> {

    }
}
