use schema_registry_client::{
    CachedSchemaRegistryClient, SchemaRegistryClient, SchemaType, UnregisteredSchema,
};

#[tokio::test]
async fn can_register_avro_schema() {
    let client = CachedSchemaRegistryClient::from_url("http://localhost:8081").unwrap();

    for i in 0..100 {
        let schema = r#"
            {
                "type": "record",
                "name": "User",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "favorite_number",  "type": ["int", "null"]},
                    {"name": "favorite_color", "type": ["string", "null"]}
                ]
            }
            "#;

        let subject = format!("test-avro-subject-{}-value", i);
        let schema = UnregisteredSchema::schema(schema).schema_type(SchemaType::Avro);

        client.register_schema(&subject, &schema).await.unwrap();
    }
}

#[tokio::test]
async fn can_register_proto_schema() {
    let client = CachedSchemaRegistryClient::from_url("http://localhost:8081").unwrap();

    for i in 0..100 {
        let schema = r#"
            syntax = "proto3";

            package test;

            message Test {
                string name = 1;
                int32 age = 2;
            }
            "#;

        let subject = format!("test-proto-subject-{}-value", i);
        let schema = UnregisteredSchema::schema(schema).schema_type(SchemaType::Protobuf);

        client.register_schema(&subject, &schema).await.unwrap();
    }
}

#[tokio::test]
async fn can_register_json_schema() {
    let client = CachedSchemaRegistryClient::from_url("http://localhost:8081").unwrap();

    for i in 0..100 {
        let schema = r#"
            {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string"
                    },
                    "age": {
                        "type": "integer"
                    }
                }
            }
            "#;

        let subject = format!("test-json-subject-{}-value", i);
        let schema = UnregisteredSchema::schema(schema).schema_type(SchemaType::Json);

        client.register_schema(&subject, &schema).await.unwrap();
    }
}
