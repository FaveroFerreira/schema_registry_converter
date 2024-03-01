use std::fmt::Display;
use std::sync::Arc;

use async_trait::async_trait;

use schema_registry_converter::client::{
    Schema, SchemaRegistryClient, SchemaRegistryError, UnregisteredSchema, Version,
};
use schema_registry_converter::serde::avro::SchemaRegistryAvroDeserializer;

pub struct MySchemaRegistryClient {
    // ...
}

#[derive(Debug)]
pub struct MyError {}

impl Display for MyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MyError")
    }
}
impl std::error::Error for MyError {}

impl From<MyError> for SchemaRegistryError {
    fn from(error: MyError) -> Self {
        SchemaRegistryError::Other(Box::new(error))
    }
}

#[async_trait]
impl SchemaRegistryClient for MySchemaRegistryClient {
    async fn get_schema_by_subject(
        &self,
        _subject: &str,
        _version: Version,
    ) -> Result<Schema, SchemaRegistryError> {
        Err(MyError {})?
    }

    async fn get_schema_by_id(&self, _id: u32) -> Result<Schema, SchemaRegistryError> {
        Err(MyError {})?
    }

    async fn register_schema(
        &self,
        _subject: &str,
        _unregistered: &UnregisteredSchema,
    ) -> Result<Schema, SchemaRegistryError> {
        Err(MyError {})?
    }
}

#[tokio::main]
async fn main() {
    let _de = SchemaRegistryAvroDeserializer::new(Arc::new(MySchemaRegistryClient {}));
}
