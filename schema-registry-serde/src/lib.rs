use std::error::Error as StdError;
use std::fmt::{Display, Formatter};
use std::ops::Range;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;

pub const MAGIC_BYTE: u8 = 0;
pub const ENCODED_ID_RANGE: Range<usize> = 1..5;
pub const PAYLOAD_OFFSET: usize = 5;

pub struct Extracted<'a> {
    pub schema_id: u32,
    pub payload: &'a [u8],
}

#[derive(Debug, Clone, Copy)]
pub enum ExtractError {
    EmptyData,
    InvalidMagicByte,
    InvalidDataLength,
}

impl Display for ExtractError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ExtractError::EmptyData => write!(f, "No data to extract"),
            ExtractError::InvalidMagicByte => write!(f, "Invalid magic byte, expected 0. Data may be corrupted due to incorrect serialization"),
            ExtractError::InvalidDataLength => write!(f, "Invalid data length"),
        }
    }
}

impl StdError for ExtractError {}

pub fn extract_id_and_payload(data: Option<&[u8]>) -> Result<Extracted<'_>, ExtractError> {
    let data = data.ok_or(ExtractError::EmptyData)?;

    if data.len() < 5 {
        return Err(ExtractError::InvalidDataLength);
    }

    if data[0] != MAGIC_BYTE {
        return Err(ExtractError::InvalidMagicByte);
    }

    let schema_id = &data[ENCODED_ID_RANGE];
    let payload = &data[PAYLOAD_OFFSET..];

    Ok(Extracted {
        schema_id: u32::from_be_bytes([schema_id[0], schema_id[1], schema_id[2], schema_id[3]]),
        payload,
    })
}

pub fn insert_magic_byte_and_id(id: u32, payload: &[u8]) -> Vec<u8> {
    let mut buf = vec![0u8; 5];
    buf[0] = MAGIC_BYTE;
    buf[1..5].copy_from_slice(&id.to_be_bytes());
    buf.extend_from_slice(payload);
    buf
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SubjectNameStrategy<'a> {
    TopicName(&'a str),
    RecordName(&'a str),
    TopicRecordName(&'a str, &'a str),
    SubjectName(&'a str),
}

impl SubjectNameStrategy<'_> {
    pub fn key(&self) -> String {
        match self {
            SubjectNameStrategy::TopicName(topic) => format!("{}-key", topic),
            SubjectNameStrategy::RecordName(record) => format!("{}-key", record),
            SubjectNameStrategy::TopicRecordName(topic, record) => {
                format!("{}-{}-key", topic, record)
            }
            SubjectNameStrategy::SubjectName(subject) => subject.to_string(),
        }
    }

    pub fn value(&self) -> String {
        match self {
            SubjectNameStrategy::TopicName(topic) => format!("{}-value", topic),
            SubjectNameStrategy::RecordName(record) => format!("{}-value", record),
            SubjectNameStrategy::TopicRecordName(topic, record) => {
                format!("{}-{}-value", topic, record)
            }
            SubjectNameStrategy::SubjectName(subject) => subject.to_string(),
        }
    }
}

#[async_trait]
pub trait SchemaRegistrySerializer: Send + Sync {
    type Error: StdError + Send + Sync;

    async fn serialize_value<T>(
        &self,
        strategy: SubjectNameStrategy<'_>,
        data: &T,
    ) -> Result<Vec<u8>, Self::Error>
    where
        T: Serialize + Send + Sync;

    async fn serialize_key<T>(
        &self,
        strategy: SubjectNameStrategy<'_>,
        data: &T,
    ) -> Result<Vec<u8>, Self::Error>
    where
        T: Serialize + Send + Sync;
}

#[async_trait]
pub trait SchemaRegistryDeserializer: Send + Sync {
    type Error: StdError + Send + Sync;

    async fn deserialize<T>(&self, data: Option<&[u8]>) -> Result<T, Self::Error>
    where
        T: DeserializeOwned;
}
