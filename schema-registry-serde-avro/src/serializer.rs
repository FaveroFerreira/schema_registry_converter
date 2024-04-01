use std::sync::Arc;

use apache_avro::Schema as AvroSchema;
use async_trait::async_trait;
use serde::Serialize;

use schema_registry_client::{SchemaRegistryClient, Version};
use schema_registry_serde::insert_magic_byte_and_id;
use schema_registry_serde::SchemaRegistrySerializer;
use schema_registry_serde::SubjectNameStrategy;

use crate::error::AvroSerializationError;

pub struct SchemaRegistryAvroSerializer {
    schema_registry_client: Arc<dyn SchemaRegistryClient>,
}

impl SchemaRegistryAvroSerializer {
    pub fn new(schema_registry_client: Arc<dyn SchemaRegistryClient>) -> Self {
        Self {
            schema_registry_client,
        }
    }
}

#[async_trait]
impl SchemaRegistrySerializer for SchemaRegistryAvroSerializer {
    type Error = AvroSerializationError;

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

        let avro_schema = AvroSchema::parse_str(&schema.schema)?;
        let avro_value = apache_avro::to_value(data)?;

        let data = apache_avro::to_avro_datum(&avro_schema, avro_value)?;

        Ok(insert_magic_byte_and_id(schema.id, &data))
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

        let avro_schema = AvroSchema::parse_str(&schema.schema)?;
        let avro_value = apache_avro::to_value(data)?;

        let data = apache_avro::to_avro_datum(&avro_schema, avro_value)?;

        Ok(insert_magic_byte_and_id(schema.id, &data))
    }
}
