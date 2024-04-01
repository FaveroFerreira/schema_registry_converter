use std::error::Error as StdError;
use std::fmt::{Display, Formatter};
use std::ops::Range;

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
