//! # Schema Registry Serde
//!
//! This crate provides the core types and traits for working schema registry serialization and deserialization.

mod deserializer;
mod payload;
mod serializer;

pub use deserializer::*;
pub use payload::*;
pub use serializer::*;
