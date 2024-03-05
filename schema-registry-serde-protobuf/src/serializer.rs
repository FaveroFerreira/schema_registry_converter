use async_trait::async_trait;
use serde::Serialize;

use schema_registry_serde::{SchemaRegistrySerializer, SubjectNameStrategy};

use crate::error::ProtoSerializationError;

pub struct SchemaRegistryProtoSerializer {}

#[async_trait]
impl SchemaRegistrySerializer for SchemaRegistryProtoSerializer {
    type Error = ProtoSerializationError;

    async fn serialize_value<T>(
        &self,
        strategy: SubjectNameStrategy<'_>,
        data: &T,
    ) -> Result<Vec<u8>, Self::Error>
    where
        T: Serialize + Send + Sync,
    {
        unimplemented!()
    }

    async fn serialize_key<T>(
        &self,
        strategy: SubjectNameStrategy<'_>,
        data: &T,
    ) -> Result<Vec<u8>, Self::Error>
    where
        T: Serialize + Send + Sync,
    {
        unimplemented!()
    }
}
