use async_trait::async_trait;

use crate::error::SchemaRegistryError;

mod cached;
mod util;

#[async_trait]
pub trait SchemaRegistryClient {
    async fn get_schema_by_id(&self, id: u32) -> Result<(), SchemaRegistryError>;
}
