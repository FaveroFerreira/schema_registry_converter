use std::sync::Arc;

use async_trait::async_trait;
use jsonschema::JSONSchema;
use serde::Serialize;

use schema_registry_client::{SchemaRegistryClient, Version};
use schema_registry_serde::{
    insert_magic_byte_and_id, SchemaRegistrySerializer, SubjectNameStrategy,
};

use crate::error::JsonSerializationError;

pub struct SchemaRegistryJsonSerializer {
    schema_registry_client: Arc<dyn SchemaRegistryClient>,
}

impl SchemaRegistryJsonSerializer {
    pub fn new(schema_registry_client: Arc<dyn SchemaRegistryClient>) -> Self {
        Self {
            schema_registry_client,
        }
    }
}

#[async_trait]
impl SchemaRegistrySerializer for SchemaRegistryJsonSerializer {
    type Error = JsonSerializationError;

    async fn serialize_value<T>(
        &self,
        strategy: SubjectNameStrategy<'_>,
        data: &T,
    ) -> Result<Vec<u8>, Self::Error>
    where
        T: Serialize + Send + Sync,
    {
        let subject = strategy.value();

        let schema = self
            .schema_registry_client
            .get_schema_by_subject(&subject, Version::Latest)
            .await?;

        let parsed_schema = serde_json::from_str(&schema.schema)?;
        let compiled_schema = JSONSchema::compile(&parsed_schema)?;

        let data = serde_json::to_value(data)?;
        let bytes = serde_json::to_vec(&data)?;

        compiled_schema
            .validate(&data)
            .map_err(JsonSerializationError::from)?;

        Ok(insert_magic_byte_and_id(schema.id, &bytes))
    }

    async fn serialize_key<T>(
        &self,
        strategy: SubjectNameStrategy<'_>,
        data: &T,
    ) -> Result<Vec<u8>, Self::Error>
    where
        T: Serialize + Send + Sync,
    {
        let subject = strategy.key();

        let schema = self
            .schema_registry_client
            .get_schema_by_subject(&subject, Version::Latest)
            .await?;

        let parsed_schema = serde_json::from_str(&schema.schema)?;
        let compiled_schema = JSONSchema::compile(&parsed_schema)?;

        let data = serde_json::to_value(data)?;
        let bytes = serde_json::to_vec(&data)?;

        compiled_schema
            .validate(&data)
            .map_err(JsonSerializationError::from)?;

        Ok(insert_magic_byte_and_id(schema.id, &bytes))
    }
}
