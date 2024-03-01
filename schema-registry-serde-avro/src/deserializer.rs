use std::io::Cursor;
use std::ops::Range;
use std::sync::Arc;

use apache_avro::Schema as AvroSchema;
use async_trait::async_trait;
use byteorder::{BigEndian, ReadBytesExt};
use serde::de::DeserializeOwned;

use schema_registry_client::SchemaRegistryClient;
use schema_registry_serde::SchemaRegistryDeserializer;

const AVRO_MAGIC_BYTE: u8 = 0;
const AVRO_ENCODED_SCHEMA_ID_RANGE: Range<usize> = 1..5;

pub struct Extracted<'a> {
    pub schema_id: u32,
    pub payload: &'a [u8],
}

pub struct SchemaRegistryAvroDeserializer {
    schema_registry_client: Arc<dyn SchemaRegistryClient>,
}

#[async_trait]
impl SchemaRegistryDeserializer for SchemaRegistryAvroDeserializer {
    async fn deserialize<T>(&self, data: Option<&[u8]>) -> Result<T, ()>
    where
        T: DeserializeOwned,
    {
        let extracted = extract_id_and_payload(data)?;

        let schema = self
            .schema_registry_client
            .get_schema_by_id(extracted.schema_id)
            .await
            .map_err(|_| ())?;

        // maybe cache a parsed schema?
        let schema = AvroSchema::parse_str(&schema.schema).map_err(|_| ())?;
        let mut reader = Cursor::new(extracted.payload);

        let avro_value =
            apache_avro::from_avro_datum(&schema, &mut reader, None).map_err(|_| ())?;

        let t = apache_avro::from_value(&avro_value).map_err(|_| ())?;

        Ok(t)
    }
}

fn extract_id_and_payload(data: Option<&[u8]>) -> Result<Extracted, ()> {
    match data {
        None => Err(()),
        Some(p) if p.len() > 4 && p[0] == AVRO_MAGIC_BYTE => {
            let mut buf = &p[AVRO_ENCODED_SCHEMA_ID_RANGE];
            let id = buf.read_u32::<BigEndian>().map_err(|_| ())?;
            Ok(Extracted {
                schema_id: id,
                payload: &p[AVRO_ENCODED_SCHEMA_ID_RANGE.end..],
            })
        }
        _ => Err(()),
    }
}
