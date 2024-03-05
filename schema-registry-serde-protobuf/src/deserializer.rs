use std::sync::Arc;

use serde::de::DeserializeOwned;

use schema_registry_client::SchemaRegistryClient;
use schema_registry_serde::SchemaRegistryDeserializer;

use crate::error::ProtoDeserializationError;

pub struct SchemaRegistryProtoDeserializer {
    schema_registry_client: Arc<dyn SchemaRegistryClient>,
}

impl SchemaRegistryDeserializer for SchemaRegistryProtoDeserializer {
    type Error = ProtoDeserializationError;

    async fn deserialize<T>(&self, data: Option<&[u8]>) -> Result<T, Self::Error>
    where
        T: DeserializeOwned,
    {
        unimplemented!()
    }
}
