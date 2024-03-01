use async_trait::async_trait;
use schema_registry_client::SchemaRegistryClient;
use schema_registry_serde::{SchemaRegistrySerializer, SubjectNameStrategy};
use serde::Serialize;
use std::sync::Arc;

pub struct SchemaRegistryAvroSerializer {
    schema_registry_client: Arc<dyn SchemaRegistryClient>,
}

#[async_trait]
impl SchemaRegistrySerializer for SchemaRegistryAvroSerializer {
    async fn serialize_value<T>(
        &self,
        strategy: SubjectNameStrategy<'_>,
        data: &T,
    ) -> Result<Vec<u8>, ()>
    where
        T: Serialize,
    {
        todo!()
    }

    async fn serialize_key<T>(
        &self,
        strategy: SubjectNameStrategy<'_>,
        data: &T,
    ) -> Result<Vec<u8>, ()>
    where
        T: Serialize,
    {
        todo!()
    }
}
