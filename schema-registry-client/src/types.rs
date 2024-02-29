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
