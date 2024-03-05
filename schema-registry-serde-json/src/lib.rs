mod deserializer;
mod error;
mod serializer;

pub mod prelude {
    pub mod serializer {
        pub use crate::serializer::SchemaRegistryJsonSerializer;
    }

    pub mod deserializer {
        pub use crate::deserializer::SchemaRegistryJsonDeserializer;
    }

    pub mod error {
        pub use crate::error::{JsonDeserializationError, JsonSerializationError};
    }
}
