use std::sync::Arc;

use apache_avro::Schema as AvroSchema;
use async_trait::async_trait;
use byteorder::{BigEndian, ByteOrder};
use serde::Serialize;

use schema_registry_client::{SchemaRegistryClient, Version};
use schema_registry_serde::{SchemaRegistrySerializer, SubjectNameStrategy};

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
        let subject = match strategy {
            SubjectNameStrategy::TopicName(topic) => format!("{}-value", topic),
            SubjectNameStrategy::RecordName(record) => format!("{}-value", record),
            SubjectNameStrategy::TopicRecordName(topic, record) => {
                format!("{}-{}-value", topic, record)
            }
        };

        let schema = self
            .schema_registry_client
            .get_schema_by_subject(&subject, Version::Latest)
            .await?;

        let avro_schema = AvroSchema::parse_str(&schema.schema)?;
        let avro_value = apache_avro::to_value(data)?;

        let bytes = apache_avro::to_avro_datum(&avro_schema, avro_value)?;

        let mut payload = vec![0u8];
        let mut buf = [0u8; 4];
        BigEndian::write_u32(&mut buf, schema.id);
        payload.extend_from_slice(&buf);
        payload.extend_from_slice(&bytes);

        Ok(payload)
    }

    async fn serialize_key<T>(
        &self,
        strategy: SubjectNameStrategy<'_>,
        data: &T,
    ) -> Result<Vec<u8>, Self::Error>
    where
        T: Serialize + Send + Sync,
    {
        let subject = match strategy {
            SubjectNameStrategy::TopicName(topic) => format!("{}-key", topic),
            SubjectNameStrategy::RecordName(record) => format!("{}-key", record),
            SubjectNameStrategy::TopicRecordName(topic, record) => {
                format!("{}-{}-key", topic, record)
            }
        };

        let schema = self
            .schema_registry_client
            .get_schema_by_subject(&subject, Version::Latest)
            .await?;

        let avro_schema = AvroSchema::parse_str(&schema.schema)?;
        let avro_value = apache_avro::to_value(data)?;

        let bytes = apache_avro::to_avro_datum(&avro_schema, avro_value)?;

        Ok(bytes)
    }
}
