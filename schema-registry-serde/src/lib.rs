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
    async fn serialize_value<T>(
        &self,
        strategy: SubjectNameStrategy<'_>,
        data: &T,
    ) -> Result<Vec<u8>, ()>
    where
        T: Serialize;

    async fn serialize_key<T>(
        &self,
        strategy: SubjectNameStrategy<'_>,
        data: &T,
    ) -> Result<Vec<u8>, ()>
    where
        T: Serialize;
}

#[async_trait]
pub trait SchemaRegistryDeserializer: Send + Sync {
    async fn deserialize<T>(&self, data: Option<&[u8]>) -> Result<T, ()>
    where
        T: DeserializeOwned;
}
