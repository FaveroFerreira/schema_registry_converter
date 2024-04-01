use std::error::Error as StdError;

use async_trait::async_trait;
use serde::de::DeserializeOwned;

/// This trait represents the ability to deserialize a key or value from a byte array.
#[async_trait]
pub trait SchemaRegistryDeserializer: Send + Sync {
    type Error: StdError + Send + Sync;

    async fn deserialize<T>(&self, data: Option<&[u8]>) -> Result<T, Self::Error>
    where
        T: DeserializeOwned;
}
