use integer_encoding::VarInt;
use schema_registry_serde::insert_magic_byte_and_id;

use crate::error::ProtoSerializationError;

use self::resolver::IndexResolver;

pub mod compiler;
pub mod resolver;
pub mod types;

pub(crate) fn to_bytes(
    id: u32,
    resolver: &IndexResolver,
    bytes: &[u8],
    full_name: &str,
) -> Result<Vec<u8>, ProtoSerializationError> {
    let mut index_bytes = match resolver.find_index(full_name) {
        Some(v) if v.len() == 1 && v[0] == 0i32 => vec![0u8],
        Some(v) => {
            let mut result = (v.len() as i32).encode_var_vec();
            for i in &*v {
                result.append(&mut i.encode_var_vec())
            }
            result
        }
        None => {
            return Err(ProtoSerializationError::UnresolvableMessageType(
                full_name.to_string(),
            ))
        }
    };
    index_bytes.extend(bytes);
    Ok(insert_magic_byte_and_id(id, &index_bytes))
}

pub(crate) fn to_bytes_single_message(
    id: u32,
    resolver: &IndexResolver,
    bytes: &[u8],
) -> Result<Vec<u8>, ProtoSerializationError> {
    if resolver.is_single_message() {
        let mut index_bytes = vec![0u8];
        index_bytes.extend(bytes);
        Ok(insert_magic_byte_and_id(id, &index_bytes))
    } else {
        Err(ProtoSerializationError::DynamicMessage(
            "Schema was not a single message schema".to_string(),
        ))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct EncodeContext {
    pub(crate) id: u32,
    pub(crate) resolver: IndexResolver,
}

impl EncodeContext {
    pub(crate) fn new(id: u32, schema: &str) -> Self {
        Self {
            id,
            resolver: IndexResolver::new(schema),
        }
    }
}
