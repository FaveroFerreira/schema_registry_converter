use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum ProtoDeserializationError {}

#[derive(Debug, ThisError)]
pub enum ProtoSerializationError {}
