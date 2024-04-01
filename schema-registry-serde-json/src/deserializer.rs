use std::sync::Arc;

use async_trait::async_trait;
use serde::de::DeserializeOwned;

use schema_registry_client::SchemaRegistryClient;
use schema_registry_serde::SchemaRegistryDeserializer;
use schema_registry_serde::extract_id_and_payload;

use crate::error::JsonDeserializationError;

#[derive(Clone)]
pub struct SchemaRegistryJsonDeserializer {
    _schema_registry_client: Arc<dyn SchemaRegistryClient>,
}

impl SchemaRegistryJsonDeserializer {
    pub fn new(schema_registry_client: Arc<dyn SchemaRegistryClient>) -> Self {
        Self {
            _schema_registry_client: schema_registry_client,
        }
    }
}

#[async_trait]
impl SchemaRegistryDeserializer for SchemaRegistryJsonDeserializer {
    type Error = JsonDeserializationError;

    async fn deserialize<T>(&self, data: Option<&[u8]>) -> Result<T, Self::Error>
    where
        T: DeserializeOwned,
    {
        let extracted = extract_id_and_payload(data)?;

        let t = serde_json::from_slice(extracted.payload)?;

        Ok(t)
    }
}
