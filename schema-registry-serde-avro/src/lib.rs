mod deserializer;
mod error;
mod serializer;

mod prelude {
    mod serializer {
        pub use crate::serializer::SchemaRegistryAvroSerializer;
        pub use schema_registry_serde::SubjectNameStrategy;
    }

    pub use crate::deserializer::SchemaRegistryAvroDeserializer;
    pub use crate::error::{AvroDeserializationError, AvroSerializationError};
    pub use serializer::*;
}

pub use prelude::*;
