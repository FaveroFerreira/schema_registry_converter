use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use futures::future::BoxFuture;
use futures::FutureExt;
use reqwest::Client;

use crate::client::{util, SchemaRegistryClient};
use crate::config::SchemaRegistryConfig;
use crate::error::SchemaRegistryError;

pub struct CachedSchemaRegistryClient {
    urls: Arc<[String]>,
    http: Client,
    cache: DashMap<u32, String>,
}

impl CachedSchemaRegistryClient {
    pub fn from_conf(conf: SchemaRegistryConfig) -> Result<Self, SchemaRegistryError> {
        let urls = Arc::from(conf.urls.clone());
        let http = util::build_http_client(&conf)?;
        let cache = DashMap::new();

        Ok(Self { http, urls, cache })
    }

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
    async fn get_schema_by_id(&self, id: u32) -> Result<(), SchemaRegistryError> {
        let mut calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/schemas/ids/{}", url, id);

            let call: BoxFuture<Result<String, SchemaRegistryError>> = async move {
                let response = http.get(&url).send().await?.json::<String>().await?;
                Ok(response)
            }
            .boxed();

            calls.push(call.boxed());
        }

        let response = self.exec_calls(calls).await?;

        Ok(())
    }
}
