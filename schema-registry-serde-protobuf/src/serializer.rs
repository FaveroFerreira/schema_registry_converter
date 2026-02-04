use std::sync::Arc;

use async_trait::async_trait;
use prost::Message;
use prost_reflect::DynamicMessage;
use serde::Serialize;

use schema_registry_client::{Schema, SchemaRegistryClient, Version};
use schema_registry_serde::SchemaRegistrySerializer;
use schema_registry_serde::SubjectNameStrategy;

use crate::error::ProtoSerializationError;
use crate::proto::compiler::build_descriptor_pool;
use crate::proto::resolver::{IndexResolver, MessageResolver};
use crate::proto::types::add_common_files;
use crate::proto::{to_bytes, to_bytes_single_message, EncodeContext};

pub struct SchemaRegistryProtoSerializer {
    schema_registry_client: Arc<dyn SchemaRegistryClient>,
}

impl SchemaRegistryProtoSerializer {
    pub fn new(schema_registry_client: Arc<dyn SchemaRegistryClient>) -> Self {
        Self {
            schema_registry_client,
        }
    }

    async fn serialize_with_subject<T>(
        &self,
        subject: String,
        data: &T,
    ) -> Result<Vec<u8>, ProtoSerializationError>
    where
        T: Serialize + Send + Sync,
    {
        // 1. Fetch schema from registry
        let schema = self
            .schema_registry_client
            .get_schema_by_subject(&subject, Version::Latest)
            .await?;

        // 2. Collect all schema files (references + common imports + main schema)
        let schema_files = self.collect_schema_files(&schema).await?;

        // 3. Build descriptor pool from proto schemas
        let descriptor_pool =
            build_descriptor_pool(&schema_files).map_err(ProtoSerializationError::SchemaParse)?;

        // 4. Create encode context
        let encode_context = EncodeContext::new(schema.id, &schema.schema);

        // 5. Get the primary message descriptor
        let message_descriptor =
            self.get_primary_message_descriptor(&descriptor_pool, &schema.schema)?;

        // 6. Convert serde data to JSON then to DynamicMessage
        let json_value = serde_json::to_value(data)?;
        let dynamic_message =
            DynamicMessage::deserialize(message_descriptor.clone(), json_value)
                .map_err(|e| ProtoSerializationError::DynamicMessage(e.to_string()))?;

        // 7. Encode to protobuf bytes
        let proto_bytes = dynamic_message.encode_to_vec();

        // 8. Build wire format with message index
        let full_name = message_descriptor.full_name();
        if encode_context.resolver.is_single_message() {
            to_bytes_single_message(encode_context.id, &encode_context.resolver, &proto_bytes)
        } else {
            to_bytes(
                encode_context.id,
                &encode_context.resolver,
                &proto_bytes,
                full_name,
            )
        }
    }

    async fn collect_schema_files(
        &self,
        schema: &Schema,
    ) -> Result<Vec<(String, String)>, ProtoSerializationError> {
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
            // Extract filename from the proto content or generate one
            let filename = extract_proto_filename(&content, idx);
            files.push((filename, content));
        }

        // Add main schema last
        files.push(("main.proto".to_string(), schema.schema.to_string()));

        Ok(files)
    }

    fn get_primary_message_descriptor(
        &self,
        pool: &prost_reflect::DescriptorPool,
        schema: &str,
    ) -> Result<prost_reflect::MessageDescriptor, ProtoSerializationError> {
        let resolver = IndexResolver::new(schema);

        // Get the first message (index [0])
        if let Some(name) = resolver.find_index_name(&[0]) {
            pool.get_message_by_name(&name)
                .ok_or_else(|| ProtoSerializationError::UnresolvableMessageType(name.to_string()))
        } else {
            // Try to find any message in the pool from this schema
            for message in pool.all_messages() {
                // Return the first non-Google message
                if !message.full_name().starts_with("google.") {
                    return Ok(message);
                }
            }
            Err(ProtoSerializationError::SchemaParse(
                "No message found in schema".to_string(),
            ))
        }
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
impl SchemaRegistrySerializer for SchemaRegistryProtoSerializer {
    type Error = ProtoSerializationError;

    async fn serialize_value<T>(
        &self,
        strategy: SubjectNameStrategy<'_>,
        data: &T,
    ) -> Result<Vec<u8>, Self::Error>
    where
        T: Serialize + Send + Sync,
    {
        self.serialize_with_subject(strategy.value(), data).await
    }

    async fn serialize_key<T>(
        &self,
        strategy: SubjectNameStrategy<'_>,
        data: &T,
    ) -> Result<Vec<u8>, Self::Error>
    where
        T: Serialize + Send + Sync,
    {
        self.serialize_with_subject(strategy.key(), data).await
    }
}
