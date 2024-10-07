use std::sync::Arc;

use apache_avro::Schema as AvroSchema;
use async_trait::async_trait;
use serde::Serialize;

use schema_registry_client::{Schema, SchemaRegistryClient, Version};
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

        let id = schema.id;

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

        let write_schema = parsed_schemas
            .pop()
            .ok_or(AvroSerializationError::SchemaNotFound)?;
        let schemata = parsed_schemas.iter().map(|s| s).collect();
        let avro_value = apache_avro::to_value(data)?;

        let data = apache_avro::to_avro_datum_schemata(&write_schema, schemata, avro_value)?;

        Ok(insert_magic_byte_and_id(id, &data))
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
