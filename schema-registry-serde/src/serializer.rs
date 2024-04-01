use std::error::Error as StdError;

use async_trait::async_trait;
use serde::Serialize;

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
