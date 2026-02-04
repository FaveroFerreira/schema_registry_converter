use std::collections::HashMap;

use prost_reflect::DescriptorPool;
use protox::file::{ChainFileResolver, File, FileResolver, GoogleFileResolver};
use protox::Error as ProtoxError;

/// A file resolver that resolves files from an in-memory map.
pub struct InMemoryFileResolver {
    files: HashMap<String, String>,
}

impl InMemoryFileResolver {
    pub fn new(files: HashMap<String, String>) -> Self {
        Self { files }
    }
}

impl FileResolver for InMemoryFileResolver {
    fn open_file(&self, name: &str) -> Result<File, ProtoxError> {
        if let Some(content) = self.files.get(name) {
            File::from_source(name, content)
        } else {
            Err(ProtoxError::file_not_found(name))
        }
    }
}

/// Build a DescriptorPool from a list of proto schema files.
///
/// The files should be a list of (filename, content) pairs.
/// The last file in the list is considered the "main" file.
pub fn build_descriptor_pool(schema_files: &[(String, String)]) -> Result<DescriptorPool, String> {
    if schema_files.is_empty() {
        return Err("No schema files provided".to_string());
    }

    // Build file map
    let mut files_map: HashMap<String, String> = HashMap::new();
    for (filename, content) in schema_files {
        files_map.insert(filename.clone(), content.clone());
    }

    // Create chain resolver: first try in-memory, then Google well-known types
    let in_memory_resolver = InMemoryFileResolver::new(files_map);
    let google_resolver = GoogleFileResolver::new();
    let mut resolver = ChainFileResolver::new();
    resolver.add(in_memory_resolver);
    resolver.add(google_resolver);

    // Create compiler with our custom resolver
    let mut compiler = protox::Compiler::with_file_resolver(resolver);
    compiler.include_imports(true);

    // Open the main file (last one) - it will pull in dependencies
    let main_file = &schema_files.last().unwrap().0;
    compiler.open_file(main_file).map_err(|e| e.to_string())?;

    // Get descriptor pool
    Ok(compiler.descriptor_pool())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_descriptor_pool_simple() {
        let schema = r#"
            syntax = "proto3";
            package test;
            message TestMessage {
                string name = 1;
                int32 value = 2;
            }
        "#;

        let files = vec![("test.proto".to_string(), schema.to_string())];
        let pool = build_descriptor_pool(&files).expect("Failed to build descriptor pool");

        let msg = pool.get_message_by_name("test.TestMessage");
        assert!(msg.is_some());
    }

    #[test]
    fn test_build_descriptor_pool_with_google_types() {
        let schema = r#"
            syntax = "proto3";
            package test;
            import "google/protobuf/timestamp.proto";
            message TestMessage {
                string name = 1;
                google.protobuf.Timestamp created_at = 2;
            }
        "#;

        let files = vec![("test.proto".to_string(), schema.to_string())];
        let pool = build_descriptor_pool(&files).expect("Failed to build descriptor pool");

        let msg = pool.get_message_by_name("test.TestMessage");
        assert!(msg.is_some());
    }
}
