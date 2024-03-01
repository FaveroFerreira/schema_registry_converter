pub mod serde {
    #[cfg(feature = "protobuf")]
    pub mod protobuf {
        pub use schema_registry_serde_protobuf::*;
    }

    #[cfg(feature = "json")]
    pub mod json {
        pub use schema_registry_serde_json::*;
    }

    #[cfg(feature = "avro")]
    pub mod avro {
        pub use schema_registry_serde_avro::*;
    }

    pub use schema_registry_serde::SchemaRegistryDeserializer;
    pub use schema_registry_serde::SchemaRegistrySerializer;
}

pub mod client {
    pub use schema_registry_client::*;
}
