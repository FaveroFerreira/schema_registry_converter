use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Default, Debug, Clone, Copy, Eq, PartialEq)]
pub enum Version {
    #[default]
    Latest,
    Version(u32),
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Version::Latest => write!(f, "latest"),
            Version::Version(version) => write!(f, "{}", version),
        }
    }
}

#[derive(Default, Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SchemaType {
    #[default]
    Avro,
    Protobuf,
    Json,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Schema {
    #[serde(default)]
    pub schema_type: SchemaType,
    pub schema: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Subject {
    pub id: u32,
    pub subject: String,
    pub version: u32,
    #[serde(default)]
    pub schema_type: SchemaType,
    pub schema: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaReference {
    pub name: String,
    pub subject: String,
    pub version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnregisteredSchema {
    pub schema: String,
    pub schema_type: SchemaType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub references: Option<Vec<SchemaReference>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisteredSchema {
    pub id: u32,
}

impl UnregisteredSchema {
    pub fn schema<T>(schema: T) -> Self
    where
        T: Into<String>,
    {
        Self {
            schema: schema.into(),
            schema_type: SchemaType::Avro,
            references: None,
        }
    }

    pub fn schema_type(mut self, schema_type: SchemaType) -> Self {
        self.schema_type = schema_type;
        self
    }

    pub fn references(mut self, references: Vec<SchemaReference>) -> Self {
        self.references = Some(references);
        self
    }
}
