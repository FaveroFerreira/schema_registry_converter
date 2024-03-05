use std::sync::Arc;

use async_trait::async_trait;
use jsonschema::JSONSchema;
use serde::Serialize;

use schema_registry_client::{SchemaRegistryClient, Version};
use schema_registry_serde::{
    insert_magic_byte_and_id, SchemaRegistrySerializer, SubjectNameStrategy,
};

use crate::error::JsonDeserializationError;

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
    type Error = JsonDeserializationError;

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

        let parsed_schema = serde_json::from_str(&schema.schema).unwrap();
        let compiled_schema = JSONSchema::compile(&parsed_schema).unwrap();

        let data = serde_json::to_value(data).unwrap();

        if let Err(e) = compiled_schema.validate(&data) {
            for error in e {
                println!("Validation error: {}", error);
            }

            panic!("Validation error")
        }

        Ok(insert_magic_byte_and_id(
            schema.id,
            &serde_json::to_vec(&data).unwrap(),
        ))
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

        let parsed_schema = serde_json::from_str(&schema.schema).unwrap();
        let compiled_schema = JSONSchema::compile(&parsed_schema).unwrap();

        let data = serde_json::to_value(data).unwrap();

        if let Err(e) = compiled_schema.validate(&data) {
            for error in e {
                println!("Validation error: {}", error);
            }

            panic!("Validation error");
        }

        Ok(insert_magic_byte_and_id(
            schema.id,
            &serde_json::to_vec(&data).unwrap(),
        ))
    }
}
