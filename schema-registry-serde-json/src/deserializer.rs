use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use jsonschema::{Resource, ValidationOptions};
use serde::de::DeserializeOwned;

use schema_registry_client::{Schema, SchemaRegistryClient, Version};
use schema_registry_serde::extract_id_and_payload;
use schema_registry_serde::SchemaRegistryDeserializer;

use crate::error::JsonDeserializationError;

#[derive(Clone)]
pub struct SchemaRegistryJsonDeserializer {
    schema_registry_client: Arc<dyn SchemaRegistryClient>,
}

impl SchemaRegistryJsonDeserializer {
    pub fn new(schema_registry_client: Arc<dyn SchemaRegistryClient>) -> Self {
        Self {
            schema_registry_client,
        }
    }

    async fn fetch_references(
        &self,
        schema: &Schema,
    ) -> Result<HashMap<String, Schema>, JsonDeserializationError> {
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
    ) -> Result<jsonschema::Validator, JsonDeserializationError> {
        let parsed_schema: serde_json::Value = serde_json::from_str(&schema.schema)?;

        let mut options: ValidationOptions = jsonschema::options();

        for (name, ref_schema) in references {
            let ref_value: serde_json::Value = serde_json::from_str(&ref_schema.schema)?;
            let resource = Resource::from_contents(ref_value);
            options = options.with_resource(name.clone(), resource);
        }

        options
            .build(&parsed_schema)
            .map_err(|e| JsonDeserializationError::Other(e.into()))
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

        // Fetch schema by ID from the message header
        let schema = self
            .schema_registry_client
            .get_schema_by_id(extracted.schema_id)
            .await?;

        // Parse payload as JSON value
        let json_value: serde_json::Value = serde_json::from_slice(extracted.payload)?;

        // Fetch all referenced schemas and validate
        let references = self.fetch_references(&schema).await?;
        let validator = self.build_validator(&schema, &references)?;

        validator
            .validate(&json_value)
            .map_err(|e| JsonDeserializationError::Other(e.to_string().into()))?;

        // Deserialize to target type
        let t = serde_json::from_value(json_value)?;

        Ok(t)
    }
}
