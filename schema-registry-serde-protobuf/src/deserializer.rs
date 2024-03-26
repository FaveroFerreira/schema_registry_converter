use std::sync::Arc;

use async_trait::async_trait;
use serde::de::DeserializeOwned;

use schema_registry_client::SchemaRegistryClient;
use schema_registry_serde::{extract_id_and_payload, SchemaRegistryDeserializer};

use crate::{error::ProtoDeserializationError, proto::ProtoSchema};

pub struct SchemaRegistryProtoDeserializer {
    schema_registry_client: Arc<dyn SchemaRegistryClient>,
}

#[async_trait]
impl SchemaRegistryDeserializer for SchemaRegistryProtoDeserializer {
    type Error = ProtoDeserializationError;

    async fn deserialize<T>(&self, data: Option<&[u8]>) -> Result<T, Self::Error>
    where
        T: DeserializeOwned,
    {
        let extracted = extract_id_and_payload(data)?;

        let schema = self
            .schema_registry_client
            .get_schema_by_id(extracted.schema_id)
            .await?;

        let _proto_schema = ProtoSchema::parse(&schema.schema).unwrap();

        todo!()
    }
}
