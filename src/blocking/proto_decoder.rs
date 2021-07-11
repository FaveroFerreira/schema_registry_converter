use std::collections::hash_map::{Entry, RandomState};
use std::collections::HashMap;

use bytes::Bytes;

use crate::blocking::schema_registry::{
    get_referenced_schema, get_schema_by_id_and_type, SrSettings,
};
use crate::error::SRCError;
use crate::proto_resolver::{resolve_name, to_index_and_data, MessageResolver};
use crate::schema_registry_common::{get_bytes_result, BytesResult, RegisteredSchema, SchemaType};
use protofish::decode::{MessageValue, Value};
use protofish::context::Context;

#[derive(Debug)]
pub struct ProtoDecoder {
    sr_settings: SrSettings,
    cache: HashMap<u32, Result<DecodeContext, SRCError>, RandomState>,
}

impl ProtoDecoder {
    /// Creates a new decoder which will use the supplied url used in creating the sr settings to
    /// fetch the schema's since the schema needed is encoded in the binary, independent of the
    /// SubjectNameStrategy we don't need any additional data. It's possible for recoverable errors
    /// to stay in the cache, when a result comes back as an error you can use
    /// remove_errors_from_cache to clean the cache, keeping the correctly fetched schema's
    pub fn new(sr_settings: SrSettings) -> ProtoDecoder {
        ProtoDecoder {
            sr_settings,
            cache: HashMap::new(),
        }
    }
    /// Remove al the errors from the cache, you might need to/want to run this when a recoverable
    /// error is met. Errors are also cashed to prevent trying to get schema's that either don't
    /// exist or can't be parsed.
    pub fn remove_errors_from_cache(&mut self) {
        self.cache.retain(|_, v| v.is_ok());
    }
    /// Decodes bytes into a value.
    /// The choice to use Option<&[u8]> as type us made so it plays nice with the BorrowedMessage
    /// struct from rdkafka, for example if we have m: &'a BorrowedMessage and decoder: &'a mut
    /// Decoder we can use decoder.decode(m.payload()) to decode the payload or
    /// decoder.decode(m.key()) to get the decoded key.
    pub fn decode(&mut self, bytes: Option<&[u8]>) -> Result<Value, SRCError> {
        match get_bytes_result(bytes) {
            BytesResult::Null => Ok(Value::Bytes(Bytes::new())),
            BytesResult::Valid(id, bytes) => {
                Ok(Value::Message(Box::from(self.deserialize(id, &bytes)?)))
            }
            BytesResult::Invalid(i) => Ok(Value::Bytes(Bytes::from(i))),
        }
    }
    /// The actual deserialization trying to get the id from the bytes to retrieve the schema, and
    /// using a reader transforms the bytes to a value.
    fn deserialize(&mut self, id: u32, bytes: &[u8]) -> Result<MessageValue, SRCError> {
        match self.context(id) {
            Ok(s) => {
                let (index, data) = to_index_and_data(bytes);
                let full_name = resolve_name(&s.resolver, &index)?;
                let message_info = s.context.get_message(full_name).unwrap();
                Ok(message_info.decode(&data, &s.context))
            }
            Err(e) => Err(Clone::clone(e)),
        }
    }
    /// Gets the Context object, either from the cache, or from the schema registry and then putting
    /// it into the cache.
    fn context(&mut self, id: u32) -> &Result<DecodeContext, SRCError> {
        match self.cache.entry(id) {
            Entry::Occupied(e) => &*e.into_mut(),
            Entry::Vacant(e) => {
                let v = match get_schema_by_id_and_type(id, &self.sr_settings, SchemaType::Protobuf)
                {
                    Ok(v) => to_resolve_context(&self.sr_settings, v),
                    Err(e) => Err(e.into_cache()),
                };
                &*e.insert(v)
            }
        }
    }
}

fn add_files(
    sr_settings: &SrSettings,
    registered_schema: RegisteredSchema,
    files: &mut Vec<String>,
) -> Result<(), SRCError> {
    for r in registered_schema.references {
        let child_schema = get_referenced_schema(sr_settings, &r)?;
        add_files(sr_settings, child_schema, files)?;
    }
    files.push(registered_schema.schema);
    Ok(())
}

#[derive(Debug)]
struct DecodeContext {
    resolver: MessageResolver,
    context: Context,
}

fn to_resolve_context(
    sr_settings: &SrSettings,
    registered_schema: RegisteredSchema,
) -> Result<DecodeContext, SRCError> {
    let resolver = MessageResolver::new(&registered_schema.schema);
    let mut files = Vec::new();
    add_files(sr_settings, registered_schema, &mut files)?;
    match Context::parse(&files) {
        Ok(context) => Ok(DecodeContext { resolver, context }),
        Err(e) => Err(SRCError::non_retryable_with_cause(
            e,
            "Error creating proto context",
        )),
    }
}

#[cfg(test)]
mod tests {
    use mockito::{mock, server_address};

    use crate::blocking::proto_decoder::ProtoDecoder;
    use crate::blocking::schema_registry::SrSettings;
    use protofish::decode::Value;
    use test_utils::{
        get_proto_body, get_proto_body_with_reference, get_proto_complex,
        get_proto_complex_proto_test_message, get_proto_complex_references, get_proto_hb_101,
        get_proto_hb_schema, get_proto_result,
    };

    #[test]
    fn test_decoder_default() {
        let _m = mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_proto_body(get_proto_hb_schema(), 1))
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut decoder = ProtoDecoder::new(sr_settings);
        let heartbeat = decoder.decode(Some(get_proto_hb_101()));

        let message = match heartbeat {
            Ok(Value::Message(x)) => *x,
            Err(e) => panic!("Error: {:?}, while none expected", e),
            Ok(v) => panic!("Other value: {:?} than expected Message", v),
        };

        assert_eq!(Value::UInt64(101u64), message.fields[0].value)
    }

    #[test]
    fn test_decoder_cache() {
        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut decoder = ProtoDecoder::new(sr_settings);
        let error = decoder.decode(Some(get_proto_hb_101())).unwrap_err();

        assert_eq!(true, error.cached);

        let _m = mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_proto_body(get_proto_hb_schema(), 1))
            .create();

        let error = decoder.decode(Some(get_proto_hb_101())).unwrap_err();

        assert_eq!(true, error.cached);

        decoder.remove_errors_from_cache();

        let message = match decoder.decode(Some(get_proto_hb_101())).unwrap() {
            Value::Message(x) => *x,
            v => panic!("Other value: {:?} than expected Message", v),
        };

        assert_eq!(Value::UInt64(101u64), message.fields[0].value);
    }

    #[test]
    fn test_decoder_complex() {
        let _m = mock("GET", "/schemas/ids/6?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_proto_body_with_reference(
                get_proto_complex(),
                2,
                get_proto_complex_references(),
            ))
            .create();

        let _m = mock("GET", "/subjects/result.proto/versions/1")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_proto_body(get_proto_result(), 1))
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut decoder = ProtoDecoder::new(sr_settings);
        let proto_test = decoder.decode(Some(get_proto_complex_proto_test_message()));

        let message = match proto_test {
            Ok(Value::Message(x)) => *x,
            Err(e) => panic!("Error: {:?}, while none expected", e),
            Ok(v) => panic!("Other value: {:?} than expected Message", v),
        };
        assert_eq!(message.fields[1].value, Value::Int64(1))
    }

    #[test]
    fn display_decoder() {
        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let decoder = ProtoDecoder::new(sr_settings);
        assert_eq!(
            "ProtoDecoder { sr_settings: SrSettings { urls: [\"http://127.0.0.1:1234\"], client: Client, authorization: None }, cache: {} }"
                .to_owned(),
            format!("{:?}", decoder)
        )
    }
}
