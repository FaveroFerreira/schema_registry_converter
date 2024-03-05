//! # Schema Registry Serde
//!
//! This crate provides the core types and traits for working schema registry serialization and deserialization.

use std::error::Error as StdError;
use std::fmt::{Display, Formatter};
use std::ops::Range;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;

pub const MAGIC_BYTE: u8 = 0;
pub const ENCODED_ID_RANGE: Range<usize> = 1..5;
pub const PAYLOAD_OFFSET: usize = 5;

/// Extracted schema id and payload from a message
///
/// This struct represents the result of extracting a schema id and payload from a message.
pub struct Extracted<'a> {
    pub schema_id: u32,
    pub payload: &'a [u8],
}

/// Possible errors that can occur when extracting a schema id and payload from a message
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

/// This is a helper function to extract a schema id and payload from a message
///
/// # Arguments
/// * `data` - The data to extract the schema id and payload from, it can be Kafka Key or Value.
///
/// # Errors
/// This function returns an error if the data is empty, the magic byte is invalid or the data length is invalid.
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

/// This is a helper function to insert a schema id and payload into a message
///
/// # Arguments
/// * `id` - The schema id to insert into the message
/// * `payload` - The payload to insert into the message
///
/// # Returns
///
/// A new Vec<u8> with the schema id and payload inserted
pub fn insert_magic_byte_and_id(id: u32, payload: &[u8]) -> Vec<u8> {
    let mut buf = vec![0u8; 5];
    buf[0] = MAGIC_BYTE;
    buf[1..5].copy_from_slice(&id.to_be_bytes());
    buf.extend_from_slice(payload);
    buf
}

/// This enum represents the different strategies to use when manipulating a subject in the schema registry
///
/// The strategies are:
/// * `TopicName` - Use the topic name as the subject
/// * `RecordName` - Use the record name as the subject
/// * `TopicRecordName` - Use the topic and record name as the subject
/// * `SubjectName` - Use the subject name as the subject
///
///
/// # Example
///
/// ```
/// use schema_registry_serde::SubjectNameStrategy;
///
/// let topic_name = SubjectNameStrategy::TopicName("account.created");
/// let record_name = SubjectNameStrategy::RecordName("AccountCreatedDTO");
/// let topic_record_name = SubjectNameStrategy::TopicRecordName("account.created", "AccountCreatedDTO");
/// let subject_name = SubjectNameStrategy::SubjectName("account.created");
///
/// assert_eq!(topic_name.key(), "account.created-key");
/// assert_eq!(record_name.key(), "AccountCreatedDTO-key");
/// assert_eq!(topic_record_name.key(), "account.created-AccountCreatedDTO-key");
/// assert_eq!(subject_name.key(), "account.created");
///
/// assert_eq!(topic_name.value(), "account.created-value");
/// assert_eq!(record_name.value(), "AccountCreatedDTO-value");
/// assert_eq!(topic_record_name.value(), "account.created-AccountCreatedDTO-value");
/// assert_eq!(subject_name.value(), "account.created");
/// ```
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

/// This trait represents the ability to serialize a key or value to a byte array, using a specific subject name strategy.
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

/// This trait represents the ability to deserialize a key or value from a byte array.
#[async_trait]
pub trait SchemaRegistryDeserializer: Send + Sync {
    type Error: StdError + Send + Sync;

    async fn deserialize<T>(&self, data: Option<&[u8]>) -> Result<T, Self::Error>
    where
        T: DeserializeOwned;
}
