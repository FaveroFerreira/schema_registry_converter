use std::sync::Arc;

use async_trait::async_trait;
use prost_reflect::DynamicMessage;
use serde::de::DeserializeOwned;

use schema_registry_client::{Schema, SchemaRegistryClient, Version};
use schema_registry_serde::{extract_id_and_payload, SchemaRegistryDeserializer};

use crate::error::ProtoDeserializationError;
use crate::proto::compiler::build_descriptor_pool;
use crate::proto::resolver::{resolve_name, to_index_and_data, MessageResolver};
use crate::proto::types::add_common_files;

pub struct SchemaRegistryProtoDeserializer {
    schema_registry_client: Arc<dyn SchemaRegistryClient>,
}

impl SchemaRegistryProtoDeserializer {
    pub fn new(schema_registry_client: Arc<dyn SchemaRegistryClient>) -> Self {
        Self {
            schema_registry_client,
        }
    }

    async fn collect_schema_files(
        &self,
        schema: &Schema,
    ) -> Result<Vec<(String, String)>, ProtoDeserializationError> {
        let mut files: Vec<(String, String)> = vec![];

        // Add referenced schemas first
        if let Some(references) = &schema.references {
            for reference in references {
                let ref_schema = self
                    .schema_registry_client
                    .get_schema_by_subject(&reference.subject, Version::Number(reference.version))
                    .await?;

                // Use reference name as filename
                let filename = reference.name.clone();
                files.push((filename, ref_schema.schema.to_string()));
            }
        }

        // Add common Google types if imported
        let resolver = MessageResolver::new(&schema.schema);
        let mut common_files: Vec<String> = vec![];
        add_common_files(resolver.imports(), &mut common_files);

        for (idx, content) in common_files.into_iter().enumerate() {
            let filename = extract_proto_filename(&content, idx);
            files.push((filename, content));
        }

        // Add main schema last
        files.push(("main.proto".to_string(), schema.schema.to_string()));

        Ok(files)
    }

    fn get_message_descriptor(
        &self,
        pool: &prost_reflect::DescriptorPool,
        message_name: &str,
    ) -> Result<prost_reflect::MessageDescriptor, ProtoDeserializationError> {
        pool.get_message_by_name(message_name).ok_or_else(|| {
            ProtoDeserializationError::SchemaParse(format!(
                "Message '{}' not found in schema",
                message_name
            ))
        })
    }
}

fn extract_proto_filename(content: &str, idx: usize) -> String {
    // Try to extract package name from proto content
    for line in content.lines() {
        let line = line.trim();
        if line.starts_with("package ") {
            if let Some(pkg) = line
                .strip_prefix("package ")
                .and_then(|s| s.strip_suffix(';'))
            {
                return format!("{}.proto", pkg.trim().replace('.', "/"));
            }
        }
    }
    format!("schema_{}.proto", idx)
}

#[async_trait]
impl SchemaRegistryDeserializer for SchemaRegistryProtoDeserializer {
    type Error = ProtoDeserializationError;

    async fn deserialize<T>(&self, data: Option<&[u8]>) -> Result<T, Self::Error>
    where
        T: DeserializeOwned,
    {
        // 1. Extract schema ID and payload (skipping magic byte)
        let extracted = extract_id_and_payload(data)?;

        // 2. Fetch schema from registry by ID
        let schema = self
            .schema_registry_client
            .get_schema_by_id(extracted.schema_id)
            .await?;

        // 3. Parse message index from payload and get actual proto data
        let (index, proto_data) = to_index_and_data(extracted.payload);

        // 4. Resolve message name from index
        let resolver = MessageResolver::new(&schema.schema);
        let message_name = resolve_name(&resolver, &index)
            .map_err(|_| ProtoDeserializationError::UnresolvableIndex(index.clone()))?;

        // 5. Build descriptor pool from schema files
        let schema_files = self.collect_schema_files(&schema).await?;
        let pool =
            build_descriptor_pool(&schema_files).map_err(ProtoDeserializationError::SchemaParse)?;

        // 6. Get message descriptor
        let descriptor = self.get_message_descriptor(&pool, &message_name)?;

        // 7. Decode protobuf to DynamicMessage
        let dynamic_message = DynamicMessage::decode(descriptor, proto_data.as_slice())
            .map_err(ProtoDeserializationError::ProtobufDecode)?;

        // 8. Convert DynamicMessage to serde_json::Value then to T
        let json_value = serde_json::to_value(&dynamic_message)?;
        let result: T = serde_json::from_value(json_value)?;

        Ok(result)
    }
}
