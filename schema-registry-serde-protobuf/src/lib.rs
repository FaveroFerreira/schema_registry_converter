mod deserializer;
mod error;
mod proto;
mod serializer;

pub mod prelude {
    pub mod serializer {
        pub use crate::serializer::SchemaRegistryProtoSerializer;
    }

    pub mod deserializer {
        pub use crate::deserializer::SchemaRegistryProtoDeserializer;
    }

    pub mod error {
        pub use crate::error::{ProtoDeserializationError, ProtoSerializationError};
    }
}
