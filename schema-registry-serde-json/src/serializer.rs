use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use jsonschema::{Resource, ValidationOptions};
use serde::Serialize;

use schema_registry_client::{Schema, SchemaRegistryClient, Version};
use schema_registry_serde::insert_magic_byte_and_id;
use schema_registry_serde::SchemaRegistrySerializer;
use schema_registry_serde::SubjectNameStrategy;

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

    async fn fetch_references(
        &self,
        schema: &Schema,
    ) -> Result<HashMap<String, Schema>, JsonSerializationError> {
        let mut references = HashMap::new();

        if let Some(refs) = &schema.references {
            for reference in refs {
                let ref_schema = self
                    .schema_registry_client
                    .get_schema_by_subject(&reference.subject, Version::Number(reference.version))
                    .await?;

                // Recursively fetch nested references
                let nested = Box::pin(self.fetch_references(&ref_schema)).await?;
                references.extend(nested);

                references.insert(reference.name.clone(), ref_schema);
            }
        }

        Ok(references)
    }

    fn build_validator(
        &self,
        schema: &Schema,
        references: &HashMap<String, Schema>,
    ) -> Result<jsonschema::Validator, JsonSerializationError> {
        let parsed_schema: serde_json::Value = serde_json::from_str(&schema.schema)?;

        let mut options: ValidationOptions = jsonschema::options();

        for (name, ref_schema) in references {
            let ref_value: serde_json::Value = serde_json::from_str(&ref_schema.schema)?;
            let resource = Resource::from_contents(ref_value);
            options = options.with_resource(name.clone(), resource);
        }

        options
            .build(&parsed_schema)
            .map_err(|e| JsonSerializationError::Other(e.into()))
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

        let data = serde_json::to_value(data)?;

        // Fetch all referenced schemas
        let references = self.fetch_references(&schema).await?;

        // Build validator with references
        let validator = self.build_validator(&schema, &references)?;

        validator
            .validate(&data)
            .map_err(JsonSerializationError::from)?;

        let bytes = serde_json::to_vec(&data)?;

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

        let data = serde_json::to_value(data)?;

        // Fetch all referenced schemas (keys usually don't have refs, but support it anyway)
        let references = self.fetch_references(&schema).await?;

        // Build validator with references
        let validator = self.build_validator(&schema, &references)?;

        validator
            .validate(&data)
            .map_err(JsonSerializationError::from)?;

        let bytes = serde_json::to_vec(&data)?;

        Ok(insert_magic_byte_and_id(schema.id, &bytes))
    }
}
