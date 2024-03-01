use std::io::Cursor;
use std::ops::Range;
use std::sync::Arc;

use apache_avro::Schema as AvroSchema;
use async_trait::async_trait;
use byteorder::{BigEndian, ReadBytesExt};
use serde::de::DeserializeOwned;

use crate::error::{AvroDeserializationError, ErrorMessage};
use schema_registry_client::SchemaRegistryClient;
use schema_registry_serde::SchemaRegistryDeserializer;

pub const AVRO_MAGIC_BYTE: u8 = 0;
pub const AVRO_ENCODED_SCHEMA_ID_RANGE: Range<usize> = 1..5;

pub struct Extracted<'a> {
    pub schema_id: u32,
    pub payload: &'a [u8],
}

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

fn extract_id_and_payload(data: Option<&[u8]>) -> Result<Extracted<'_>, AvroDeserializationError> {
    match data {
        None => Err(ErrorMessage::new("empty payload").into()),
        Some(p) if p.len() > 4 && p[0] == AVRO_MAGIC_BYTE => {
            let mut buf = &p[AVRO_ENCODED_SCHEMA_ID_RANGE];
            let id = buf.read_u32::<BigEndian>().map_err(|e| {
                ErrorMessage::new(format!(
                    "failed to read schema id from payload bytes: {}",
                    e
                ))
            })?;
            Ok(Extracted {
                schema_id: id,
                payload: &p[AVRO_ENCODED_SCHEMA_ID_RANGE.end..],
            })
        }
        _ => Err(ErrorMessage::new("invalid payload").into()),
    }
}
