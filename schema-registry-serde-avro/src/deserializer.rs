use std::io::Cursor;
use std::sync::Arc;

use apache_avro::Schema as AvroSchema;
use async_trait::async_trait;
use serde::de::DeserializeOwned;

use schema_registry_client::SchemaRegistryClient;
use schema_registry_serde::{extract_id_and_payload, SchemaRegistryDeserializer};

use crate::error::AvroDeserializationError;

#[derive(Clone)]
pub struct SchemaRegistryAvroDeserializer {
    schema_registry_client: Arc<dyn SchemaRegistryClient>,
}

impl SchemaRegistryAvroDeserializer {
    pub fn new(schema_registry_client: Arc<dyn SchemaRegistryClient>) -> Self {
        Self {
            schema_registry_client,
        }
    }
}

#[async_trait]
impl SchemaRegistryDeserializer for SchemaRegistryAvroDeserializer {
    type Error = AvroDeserializationError;

    async fn deserialize<T>(&self, data: Option<&[u8]>) -> Result<T, Self::Error>
    where
        T: DeserializeOwned,
    {
        let extracted = extract_id_and_payload(data)?;

        let schema = self
            .schema_registry_client
            .get_schema_by_id(extracted.schema_id)
            .await?;

        // maybe cache a parsed schema?
        let schema = AvroSchema::parse_str(&schema.schema)?;
        let mut reader = Cursor::new(extracted.payload);

        let avro_value = apache_avro::from_avro_datum(&schema, &mut reader, None)?;

        let t = apache_avro::from_value(&avro_value)?;

        Ok(t)
    }
}
