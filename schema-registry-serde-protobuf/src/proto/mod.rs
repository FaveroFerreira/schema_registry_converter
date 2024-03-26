use std::path::PathBuf;

use crate::proto::resolver::proto_resolver;

mod resolver;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Syntax {
    #[default]
    Proto2,
    Proto3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Modifier {
    Optional,
    Repeated,
    Required,
    Map,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FieldType {
    Int32,
    Int64,
    Uint32,
    Uint64,
    Sint32,
    Sint64,
    Bool,
    Enum(EnumIndex),
    Fixed64,
    Sfixed64,
    Double,
    StringCow,
    BytesCow,
    String_,
    Bytes_,
    Message(MessageIndex),
    MessageOrEnum(String),
    Fixed32,
    Sfixed32,
    Float,
    Map(Box<FieldType>, Box<FieldType>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct EnumIndex {
    msg_index: MessageIndex,
    index: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct MessageIndex {
    indexes: Vec<usize>,
}

#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub modifier: Modifier,
    pub r#type: FieldType,
    pub number: i32,
    pub default: Option<String>,
    pub packed: Option<bool>,
    pub boxed: bool,
    pub deprecated: bool,
}

#[derive(Debug, Clone, Default)]
pub struct Extend {
    /// The message being extended.
    pub name: String,
    /// All fields that are being added to the extended message.
    pub fields: Vec<Field>,
}

#[derive(Debug, Clone, Default)]
pub struct Message {
    pub name: String,
    pub fields: Vec<Field>,
    pub oneofs: Vec<OneOf>,
    pub reserved_nums: Option<Vec<i32>>,
    pub reserved_names: Option<Vec<String>>,
    pub imported: bool,
    pub package: String,        // package from imports + nested items
    pub messages: Vec<Message>, // nested messages
    pub enums: Vec<Enum>,       // nested enums
    pub module: String,         // 'package' corresponding to actual generated Rust module
    pub path: PathBuf,
    pub import: PathBuf,
    pub index: MessageIndex,
    /// Allowed extensions for this message, None if no extensions.
    pub extensions: Option<Extensions>,
}

#[derive(Debug, Clone, Default)]
pub struct Extensions {
    pub from: i32,
    /// Max number is 536,870,911 (2^29 - 1), as defined in the
    /// protobuf docs
    pub to: i32,
}

impl Extensions {
    /// The max field number that can be used as an extension.
    pub fn max() -> i32 {
        536870911
    }
}

#[derive(Debug, Clone, Default)]
pub struct Enum {
    pub name: String,
    pub fields: Vec<(String, i32)>,
    pub fully_qualified_fields: Vec<(String, i32)>,
    pub partially_qualified_fields: Vec<(String, i32)>,
    pub imported: bool,
    pub package: String,
    pub module: String,
    pub path: PathBuf,
    pub import: PathBuf,
    pub index: EnumIndex,
}

#[derive(Debug, Clone, Default)]
pub struct OneOf {
    pub name: String,
    pub fields: Vec<Field>,
    pub package: String,
    pub module: String,
    pub imported: bool,
}

#[derive(Debug, Default, Clone)]
pub struct ProtoSchema {
    pub syntax: Syntax,
    pub package: String,
    pub imports: Vec<PathBuf>,
    pub messages: Vec<Message>,
    pub extends: Vec<Extend>,
    pub enums: Vec<Enum>,
    pub module: String,
    pub owned: bool,
}

impl ProtoSchema {
    pub fn parse(schema: &str) -> Result<ProtoSchema, ()> {
        proto_resolver(schema)
            .map(|(_, proto)| proto)
            .map_err(|_| ())
    }
}
