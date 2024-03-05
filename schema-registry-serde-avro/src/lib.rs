mod deserializer;
mod error;
mod serializer;

pub mod prelude {
    pub mod serializer {
        pub use crate::serializer::SchemaRegistryAvroSerializer;
    }

    pub mod deserializer {
        pub use crate::deserializer::SchemaRegistryAvroDeserializer;
    }

    pub mod error {
        pub use crate::error::{AvroDeserializationError, AvroSerializationError};
    }
}
