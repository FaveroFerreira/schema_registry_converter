use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use futures::future::BoxFuture;
use futures::FutureExt;
use reqwest::{header, Client};

use crate::client::{util, SchemaRegistryClient};
use crate::config::SchemaRegistryConfig;
use crate::error::SchemaRegistryError;
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
            return self.get_schema_by_id(cached.value().clone()).await;
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
                    .await?
                    .json::<Subject>()
                    .await?;

                Ok(response)
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
                    .await?
                    .json::<Schema>()
                    .await?;

                Ok(response)
            }
            .boxed();

            calls.push(call.boxed());
        }

        let schema = self.exec_calls(calls).await?;

        Ok(schema)
    }
}
