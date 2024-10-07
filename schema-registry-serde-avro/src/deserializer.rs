use std::io::Cursor;
use std::sync::Arc;

use apache_avro::Schema as AvroSchema;
use async_trait::async_trait;
use serde::de::DeserializeOwned;

use schema_registry_client::{Schema, SchemaRegistryClient, Version};
use schema_registry_serde::extract_id_and_payload;
use schema_registry_serde::SchemaRegistryDeserializer;

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

        let mut schemas: Vec<Schema> = vec![];

        if let Some(references) = &schema.references {
            for reference in references {
                let reference_schema = self
                    .schema_registry_client
                    .get_schema_by_subject(&reference.subject, Version::Number(reference.version))
                    .await?;

                schemas.push(reference_schema);
            }
        }

        schemas.push(schema);

        let input = schemas
            .iter()
            .map(|s| s.schema.as_ref())
            .collect::<Vec<&str>>();

        let mut parsed_schemas = AvroSchema::parse_list(&input)?;

        let mut reader = Cursor::new(extracted.payload);

        let writer_schema = parsed_schemas
            .pop()
            .ok_or(AvroDeserializationError::SchemaNotFound)?;
        let schemata = parsed_schemas.iter().map(|s| s).collect();

        let avro_value =
            apache_avro::from_avro_datum_schemata(&writer_schema, schemata, &mut reader, None)?;

        let t = apache_avro::from_value(&avro_value)?;

        Ok(t)
    }
}
