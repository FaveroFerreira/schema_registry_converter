use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use futures::future::BoxFuture;
use futures::FutureExt;
use reqwest::{header, Client};
use serde::de::DeserializeOwned;

use crate::client::{util, SchemaRegistryClient};
use crate::config::SchemaRegistryConfig;
use crate::error::{HttpCallError, SchemaRegistryError};
use crate::types::{RegisteredSchema, Schema, Subject, UnregisteredSchema, Version};

const APPLICATION_VND_SCHEMA_REGISTRY_V1_JSON: &str = "application/vnd.schemaregistry.v1+json";

pub struct CachedSchemaRegistryClient {
    urls: Arc<[String]>,
    http: Client,
    id_cache: DashMap<u32, Schema>,
    subject_cache: DashMap<String, u32>,
}

impl CachedSchemaRegistryClient {
    /// Create a new `CachedSchemaRegistryClient` from a URL.
    ///
    /// This is the simplest way to create a new `CachedSchemaRegistryClient`.
    /// However, if you need to customize the client, you should use `from_conf` instead.
    pub fn from_url(url: &str) -> Result<Self, SchemaRegistryError> {
        let urls = Arc::from([url.to_owned()]);
        let http = util::build_http_client(&SchemaRegistryConfig::new().url(url))?;
        let id_cache = DashMap::new();
        let subject_cache = DashMap::new();

        Ok(Self {
            http,
            urls,
            id_cache,
            subject_cache,
        })
    }

    /// Create a new `CachedSchemaRegistryClient` from a `SchemaRegistryConfig`.
    ///
    /// # Errors
    ///
    /// Returns an error if the `SchemaRegistryConfig` is invalid or if the HTTP client cannot be created.
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

    /// Check if the schema is already in the cache and return it if it is.
    pub async fn check_id_cache(&self, id: u32) -> Option<Schema> {
        self.id_cache.get(&id).map(|cached| cached.value().clone())
    }

    /// Check if the subject is already in the cache and return it if it is.
    pub async fn check_subject_cache(&self, subject: &str) -> Option<u32> {
        self.subject_cache
            .get(subject)
            .map(|cached| *cached.value())
    }

    /// Insert a schema into the cache.
    pub async fn insert_id_cache(&self, id: u32, schema: Schema) {
        self.id_cache.insert(id, schema);
    }

    /// Insert a subject into the cache and update the ID cache.
    pub async fn insert_subject_cache(&self, subject: &Subject) {
        self.insert_id_cache(
            subject.id,
            Schema {
                schema_type: subject.schema_type,
                schema: subject.schema.clone(),
            },
        )
        .await;

        self.subject_cache
            .insert(subject.subject.clone(), subject.id);
    }
}

#[async_trait]
impl SchemaRegistryClient for CachedSchemaRegistryClient {
    async fn get_schema_by_subject(
        &self,
        subject: &str,
        version: Version,
    ) -> Result<Schema, SchemaRegistryError> {
        if let Some(cached) = self.check_subject_cache(subject).await {
            return self.get_schema_by_id(cached).await;
        }

        let calls = self
            .urls
            .iter()
            .map(|url| {
                let http = self.http.clone();
                let url = format!("{}/subjects/{}/versions/{}", url, subject, version);

                async move {
                    let response = http
                        .get(&url)
                        .header(header::ACCEPT, APPLICATION_VND_SCHEMA_REGISTRY_V1_JSON)
                        .send()
                        .await?;

                    parse_response::<Subject>(response).await
                }
                .boxed()
            })
            .collect();

        let subject = exec_http_calls(calls).await?;

        self.insert_subject_cache(&subject).await;

        Ok(Schema {
            schema_type: subject.schema_type,
            schema: subject.schema,
        })
    }

    async fn get_schema_by_id(&self, id: u32) -> Result<Schema, SchemaRegistryError> {
        if let Some(cached) = self.check_id_cache(id).await {
            return Ok(cached);
        }

        let calls = self
            .urls
            .iter()
            .map(|url| {
                let http = self.http.clone();
                let url = format!("{}/schemas/ids/{}?deleted=true", url, id);

                async move {
                    let response = http
                        .get(&url)
                        .header(header::ACCEPT, APPLICATION_VND_SCHEMA_REGISTRY_V1_JSON)
                        .send()
                        .await?;

                    parse_response::<Schema>(response).await
                }
                .boxed()
            })
            .collect();

        let schema = exec_http_calls(calls).await?;

        self.insert_id_cache(id, schema.clone()).await;

        Ok(schema)
    }

    async fn register_schema(
        &self,
        subject: &str,
        unregistered: &UnregisteredSchema,
    ) -> Result<Schema, SchemaRegistryError> {
        let calls = self
            .urls
            .iter()
            .map(|url| {
                let http = self.http.clone();
                let url = format!("{}/subjects/{}/versions", url, subject);

                async move {
                    let response = http
                        .post(&url)
                        .header(header::ACCEPT, APPLICATION_VND_SCHEMA_REGISTRY_V1_JSON)
                        .header(
                            header::CONTENT_TYPE,
                            APPLICATION_VND_SCHEMA_REGISTRY_V1_JSON,
                        )
                        .json(&unregistered)
                        .send()
                        .await?;

                    parse_response::<RegisteredSchema>(response).await
                }
                .boxed()
            })
            .collect();

        let registered_schema = exec_http_calls(calls).await?;

        let schema = Schema {
            schema_type: unregistered.schema_type,
            schema: unregistered.schema.clone(),
        };

        self.insert_id_cache(registered_schema.id, schema.clone())
            .await;

        Ok(schema)
    }
}

/// Execute a collection of async calls and return the first successful result.
/// If all calls fail, return the last error.
async fn exec_http_calls<T>(
    calls: Vec<BoxFuture<'_, Result<T, HttpCallError>>>,
) -> Result<T, HttpCallError> {
    let (result, remaining) = futures::future::select_ok(calls.into_iter()).await?;
    remaining.into_iter().for_each(drop);
    Ok(result)
}

/// Parse a response into a JSON value and return the result or an error.
///
/// If the response is successful, tries to parse the JSON value into the desired type.
/// If the response is not successful, tries to parse the JSON value into a `JsonValue` and return an error.
async fn parse_response<T: DeserializeOwned>(
    response: reqwest::Response,
) -> Result<T, HttpCallError> {
    let status = response.status();
    let host = response.url().to_string();
    let bytes = response.bytes().await?;

    match status.as_u16() {
        200..=299 => match serde_json::from_slice::<T>(&bytes) {
            Ok(parsed) => Ok(parsed),
            Err(source) => {
                let body = String::from_utf8_lossy(&bytes);

                Err(HttpCallError::JsonParse {
                    body: String::from(body),
                    target: std::any::type_name::<T>(),
                    source: Box::new(source),
                })
            }
        },
        _ => {
            return Err(HttpCallError::UpstreamError {
                url: host,
                status: status.as_u16(),
                body: String::from_utf8_lossy(&bytes).to_string(),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::client::cached::APPLICATION_VND_SCHEMA_REGISTRY_V1_JSON;
    use crate::client::test_util::{
        MockRequestBuilder, MockResponseBuilder, MockSchemaRegistry, HEARTBEAT_SCHEMA_FILE_PATH,
        REGISTER_SUBJECT_RESPONSE_FILE_PATH,
    };
    use crate::types::{SchemaType, UnregisteredSchema};
    use crate::{CachedSchemaRegistryClient, SchemaRegistryClient, SchemaRegistryConfig};

    mod http_components_tests {
        use http::response::Builder;
        use reqwest::{Body, Response};
        use serde::Deserialize;

        use crate::client::cached::parse_response;
        use crate::error::HttpCallError;

        #[derive(Debug, Deserialize)]
        struct TestResponse {
            status: String,
        }

        #[tokio::test]
        async fn check_can_parse_response_if_status_is_2xx() {
            let builder = Builder::new()
                .status(200)
                .body(Body::from(r#"{ "status": "OK" }"#))
                .unwrap();

            let result = parse_response::<TestResponse>(Response::from(builder)).await;

            assert!(result.is_ok());
            assert_eq!(result.unwrap().status, "OK");
        }

        #[tokio::test]
        async fn should_have_great_messages_to_help_debug_errors() {
            let builder = Builder::new()
                .status(200)
                .body(Body::from(r#"{ "malformed json" }"#))
                .unwrap();

            let result = parse_response::<TestResponse>(Response::from(builder)).await;

            let error = result.unwrap_err();

            match &error {
                HttpCallError::JsonParse { body, target, .. } => {
                    assert_eq!(body, r#"{ "malformed json" }"#);
                    assert_eq!(
                        target.to_string(),
                        "schema_registry_client::client::cached::tests::http_components_tests::TestResponse"
                    );
                    assert_eq!(error.to_string(), "Error parsing Schema Registry response '{ \"malformed json\" }' \
                    into 'schema_registry_client::client::cached::tests::http_components_tests::TestResponse': \
                    expected `:` at line 1 column 20".to_string());
                }
                _ => panic!("Expected a JSON parse error"),
            }
        }

        #[tokio::test]
        async fn should_return_client_error_if_status_is_4xx() {
            let builder = Builder::new()
                .status(400)
                .body(Body::from(r#"{ "status": "Bad Request" }"#))
                .unwrap();

            let result = parse_response::<TestResponse>(Response::from(builder)).await;

            let error = result.unwrap_err();

            match &error {
                HttpCallError::UpstreamError { status, body, .. } => {
                    assert_eq!(*status, 400);
                    assert_eq!(body, r#"{ "status": "Bad Request" }"#);
                }
                _ => panic!("Expected a client error"),
            }
        }
    }

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

    #[tokio::test]
    async fn can_register_schema_using_basic_auth() {
        let unregistered =
            UnregisteredSchema::schema(r#"{"type": "string"}"#).schema_type(SchemaType::Avro);

        let request = MockRequestBuilder::post()
            .with_path("/subjects/heartbeat/versions")
            .with_body(&unregistered)
            .with_basic_auth("sr-username", "sr-password")
            .with_header("Accept", APPLICATION_VND_SCHEMA_REGISTRY_V1_JSON)
            .with_header("Content-Type", APPLICATION_VND_SCHEMA_REGISTRY_V1_JSON);

        let response = MockResponseBuilder::status(200)
            .with_body_file(REGISTER_SUBJECT_RESPONSE_FILE_PATH)
            .with_header("Content-Type", APPLICATION_VND_SCHEMA_REGISTRY_V1_JSON);

        let sr = MockSchemaRegistry::init_mock(request, response).await;

        let config = SchemaRegistryConfig::new()
            .url(sr.url())
            .basic_auth(&"sr-username".to_owned(), &"sr-password".to_owned());

        let client = CachedSchemaRegistryClient::from_conf(config).unwrap();

        let schema = client
            .register_schema("heartbeat", &unregistered)
            .await
            .unwrap();

        assert_eq!(schema.schema_type, SchemaType::Avro);
        assert_eq!(schema.schema, r#"{"type": "string"}"#);
    }
}
