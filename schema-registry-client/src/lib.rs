mod client;
mod config;
mod error;
mod types;

mod prelude {
    pub use crate::client::cached::CachedSchemaRegistryClient;
    pub use crate::client::SchemaRegistryClient;
    pub use crate::config::SchemaRegistryConfig;
    pub use crate::error::SchemaRegistryError;
    pub use crate::types::{Schema, SchemaReference, SchemaType, UnregisteredSchema, Version};
}

pub use prelude::*;
