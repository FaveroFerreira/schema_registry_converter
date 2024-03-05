mod deserializer;
mod error;
mod serializer;

mod prelude {
    mod serializer {
        pub use crate::serializer::SchemaRegistryJsonSerializer;
        pub use schema_registry_serde::SubjectNameStrategy;
    }

    pub use crate::deserializer::SchemaRegistryJsonDeserializer;
    pub use crate::error::{JsonDeserializationError, JsonSerializationError};
    pub use serializer::*;
}

pub use prelude::*;
