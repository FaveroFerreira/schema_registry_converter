use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use futures::future::BoxFuture;
use futures::FutureExt;
use reqwest::{header, Client};
use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;

use crate::client::{util, SchemaRegistryClient};
use crate::config::SchemaRegistryConfig;
use crate::error::{HttpCallError, SchemaRegistryError};
use crate::types::{Schema, Subject, Version};

const APPLICATION_VND_SCHEMA_REGISTRY_V1_JSON: &str = "application/vnd.schemaregistry.v1+json";

pub struct CachedSchemaRegistryClient {
    urls: Arc<[String]>,
    http: Client,
    id_cache: DashMap<u32, Schema>,
    subject_cache: DashMap<String, u32>,
}

impl CachedSchemaRegistryClient {
    pub fn from_conf(conf: SchemaRegistryConfig) -> Result<Self, SchemaRegistryError> {
        let urls = Arc::from(conf.urls.clone());
        let http = util::build_http_client(&conf)?;
        let id_cache = DashMap::new();
        let subject_cache = DashMap::new();

        Ok(Self {
            http,
            urls,
            id_cache,
            subject_cache,
        })
    }

    /// Execute a list of async calls and return the first successful result.
    /// If all calls fail, the error from the last call is returned.
    async fn exec_calls<'a, T>(
        &self,
        calls: Vec<BoxFuture<'a, Result<T, SchemaRegistryError>>>,
    ) -> Result<T, SchemaRegistryError> {
        let (result, remaining) = futures::future::select_ok(calls.into_iter()).await?;
        remaining.into_iter().for_each(drop);
        Ok(result)
    }
}

#[async_trait]
impl SchemaRegistryClient for CachedSchemaRegistryClient {
    async fn get_schema_by_subject(
        &self,
        subject: &str,
        version: Version,
    ) -> Result<Schema, SchemaRegistryError> {
        if let Some(cached) = self.subject_cache.get(subject) {
            return self.get_schema_by_id(*cached.value()).await;
        }

        let mut calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/subjects/{}/versions/{}", url, subject, version);

            let call = async move {
                let response = http
                    .get(&url)
                    .header(header::ACCEPT, APPLICATION_VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await
                    .map_err(HttpCallError::from)?;

                handle_response::<Subject>(response).await
            }
            .boxed();

            calls.push(call.boxed());
        }

        let subject = self.exec_calls(calls).await?;
        self.subject_cache.insert(subject.subject, subject.id);

        let schema = Schema {
            schema_type: subject.schema_type,
            schema: subject.schema,
        };

        Ok(schema)
    }

    async fn get_schema_by_id(&self, id: u32) -> Result<Schema, SchemaRegistryError> {
        if let Some(cached) = self.id_cache.get(&id) {
            return Ok(cached.value().clone());
        }

        let mut calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/schemas/ids/{}?deleted=true", url, id);

            let call = async move {
                let response = http
                    .get(&url)
                    .header(header::ACCEPT, APPLICATION_VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await
                    .map_err(HttpCallError::from)?;

                handle_response::<Schema>(response).await
            }
            .boxed();

            calls.push(call.boxed());
        }

        let schema = self.exec_calls(calls).await?;

        Ok(schema)
    }
}

async fn handle_response<T: DeserializeOwned>(
    response: reqwest::Response,
) -> Result<T, SchemaRegistryError> {
    match response.error_for_status_ref() {
        Ok(_) => {
            let response = response.json::<T>().await.map_err(HttpCallError::from)?;
            Ok(response)
        }
        Err(source) => {
            let response = response
                .json::<JsonValue>()
                .await
                .map_err(HttpCallError::from)?;
            Err(HttpCallError::JsonParse { response, source })?
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::client::cached::APPLICATION_VND_SCHEMA_REGISTRY_V1_JSON;
    use crate::client::test_util::{
        MockRequestBuilder, MockResponseBuilder, MockSchemaRegistry, HEARTBEAT_SCHEMA_FILE_PATH,
    };
    use crate::{CachedSchemaRegistryClient, SchemaRegistryClient, SchemaRegistryConfig};

    #[tokio::test]
    async fn can_get_schema_using_basic_auth() {
        let request = MockRequestBuilder::get()
            .with_path("/schemas/ids/1")
            .with_query("deleted", "true")
            .with_basic_auth("sr-username", "sr-password")
            .with_header("Accept", APPLICATION_VND_SCHEMA_REGISTRY_V1_JSON);

        let response = MockResponseBuilder::status(200)
            .with_header("Content-Type", APPLICATION_VND_SCHEMA_REGISTRY_V1_JSON)
            .with_body_file(HEARTBEAT_SCHEMA_FILE_PATH);

        let sr = MockSchemaRegistry::init_mock(request, response).await;

        let config = SchemaRegistryConfig::new()
            .url(sr.url())
            .basic_auth(&"sr-username".to_owned(), &"sr-password".to_owned());

        let client = CachedSchemaRegistryClient::from_conf(config).unwrap();

        let _schema = client.get_schema_by_id(1).await.unwrap();
    }
}
