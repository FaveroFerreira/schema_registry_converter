use async_trait::async_trait;

use crate::error::SchemaRegistryError;
use crate::types::{Schema, UnregisteredSchema, Version};

pub mod cached;
#[cfg(test)]
pub(crate) mod test_util;
mod util;

#[async_trait]
pub trait SchemaRegistryClient: Send + Sync {
    async fn get_schema_by_subject(
        &self,
        subject: &str,
        version: Version,
    ) -> Result<Schema, SchemaRegistryError>;

    async fn get_schema_by_id(&self, id: u32) -> Result<Schema, SchemaRegistryError>;

    async fn register_schema(
        &self,
        subject: &str,
        unregistered: &UnregisteredSchema,
    ) -> Result<u32, SchemaRegistryError>;
}
