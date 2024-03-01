mod deserializer;
mod error;
mod serializer;

mod prelude {
    pub use crate::deserializer::SchemaRegistryAvroDeserializer;
    pub use crate::error::{AvroDeserializationError, AvroSerializationError};
    pub use crate::serializer::SchemaRegistryAvroSerializer;
}

pub use prelude::*;
