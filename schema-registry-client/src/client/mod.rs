use async_trait::async_trait;

use crate::error::SchemaRegistryError;
use crate::types::{Schema, Version};

pub mod cached;
mod util;

#[async_trait]
pub trait SchemaRegistryClient {
    async fn get_schema_by_subject(
        &self,
        subject: &str,
        version: Version,
    ) -> Result<Schema, SchemaRegistryError>;
    async fn get_schema_by_id(&self, id: u32) -> Result<Schema, SchemaRegistryError>;
}
