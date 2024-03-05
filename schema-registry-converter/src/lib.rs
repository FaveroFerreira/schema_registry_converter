#[cfg(feature = "protobuf")]
pub mod protobuf {
    pub use schema_registry_serde_protobuf::prelude::deserializer::*;
    pub use schema_registry_serde_protobuf::prelude::error::*;
    pub use schema_registry_serde_protobuf::prelude::serializer::*;
}

#[cfg(feature = "json")]
pub mod json {
    pub use schema_registry_serde_json::prelude::deserializer::*;
    pub use schema_registry_serde_json::prelude::error::*;
    pub use schema_registry_serde_json::prelude::serializer::*;
}

#[cfg(feature = "avro")]
pub mod avro {
    pub use schema_registry_serde_avro::prelude::deserializer::*;
    pub use schema_registry_serde_avro::prelude::error::*;
    pub use schema_registry_serde_avro::prelude::serializer::*;
}

pub use schema_registry_client::*;
pub use schema_registry_serde::SchemaRegistryDeserializer;
pub use schema_registry_serde::SchemaRegistrySerializer;
pub use schema_registry_serde::SubjectNameStrategy;
