use std::error::Error as StdError;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SubjectNameStrategy<'a> {
    TopicName(&'a str),
    RecordName(&'a str),
    TopicRecordName(&'a str, &'a str),
}

#[async_trait]
pub trait SchemaRegistrySerializer: Send + Sync {
    type Error: StdError + Send + Sync;

    async fn serialize_value<T>(
        &self,
        strategy: SubjectNameStrategy<'_>,
        data: &T,
    ) -> Result<Vec<u8>, Self::Error>
    where
        T: Serialize + Send + Sync;

    async fn serialize_key<T>(
        &self,
        strategy: SubjectNameStrategy<'_>,
        data: &T,
    ) -> Result<Vec<u8>, Self::Error>
    where
        T: Serialize + Send + Sync;
}

#[async_trait]
pub trait SchemaRegistryDeserializer: Send + Sync {
    type Error: StdError + Send + Sync;

    async fn deserialize<T>(&self, data: Option<&[u8]>) -> Result<T, Self::Error>
    where
        T: DeserializeOwned;
}
