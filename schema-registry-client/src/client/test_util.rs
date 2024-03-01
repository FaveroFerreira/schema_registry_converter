use std::fs;

use wiremock::http::Method;
use wiremock::matchers::{any, basic_auth, header, method, path, query_param};
use wiremock::{Mock, MockBuilder, MockServer, ResponseTemplate};

pub const HEARTBEAT_SCHEMA_FILE_PATH: &str = "tests/resources/heartbeat_schema.json";

#[derive(Default)]
pub struct MockRequestBuilder {
    method: Method,
    path: Option<String>,
    query: Option<Vec<(String, String)>>,
    basic_auth: Option<(String, String)>,
    headers: Vec<(String, String)>,
}

impl MockRequestBuilder {
    pub fn get() -> Self {
        Self {
            method: Method::GET,
            ..Default::default()
        }
    }

    pub fn post() -> Self {
        Self {
            method: Method::POST,
            ..Default::default()
        }
    }

    pub fn with_path(mut self, path: &str) -> Self {
        self.path = Some(path.to_owned());
        self
    }

    pub fn with_query(mut self, key: &str, value: &str) -> Self {
        let mut query = self.query.unwrap_or_default();
        query.push((key.to_owned(), value.to_owned()));
        self.query = Some(query);
        self
    }

    pub fn with_basic_auth(mut self, username: &str, password: &str) -> Self {
        self.basic_auth = Some((username.to_owned(), password.to_owned()));
        self
    }

    pub fn with_header(mut self, key: &str, value: &str) -> Self {
        self.headers.push((key.to_owned(), value.to_owned()));
        self
    }

    fn build(self) -> MockBuilder {
        let mut mock_request = Mock::given(method(self.method));

        if let Some(p) = self.path {
            mock_request = mock_request.and(path(p));
        }

        if let Some(q) = self.query {
            for (k, v) in q {
                mock_request = mock_request.and(query_param(k, v));
            }
        }

        if let Some((username, password)) = self.basic_auth {
            mock_request = mock_request.and(basic_auth(username, password));
        }

        for (k, v) in self.headers {
            mock_request = mock_request.and(header(k, v));
        }

        mock_request
    }
}

pub struct MockResponseBuilder {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body_file: Option<&'static str>,
}

impl MockResponseBuilder {
    pub fn status(status: u16) -> Self {
        Self {
            status,
            headers: vec![],
            body_file: None,
        }
    }

    pub fn with_header(mut self, key: &str, value: &str) -> Self {
        self.headers.push((key.to_owned(), value.to_owned()));
        self
    }

    pub fn with_body_file(mut self, file: &'static str) -> Self {
        self.body_file = Some(file);
        self
    }

    fn build(self) -> ResponseTemplate {
        let mut mock_response = ResponseTemplate::new(self.status);

        if let Some(file) = self.body_file {
            let content = read_file(file);
            mock_response = mock_response.set_body_string(content);
        }

        for (k, v) in self.headers {
            mock_response = mock_response.append_header(k, v);
        }

        mock_response
    }
}

pub struct MockSchemaRegistry {
    pub server: MockServer,
}

impl MockSchemaRegistry {
    pub async fn init_mock(
        req_builder: MockRequestBuilder,
        resp_builder: MockResponseBuilder,
    ) -> Self {
        let server = MockServer::start().await;

        req_builder
            .build()
            .respond_with(resp_builder.build())
            .mount(&server)
            .await;

        install_any_matcher(&server).await;

        Self { server }
    }

    pub fn url(&self) -> String {
        self.server.uri()
    }
}

pub async fn install_any_matcher(server: &MockServer) {
    Mock::given(any())
        .respond_with(
            ResponseTemplate::new(500).set_body_string(r#"{ "error": "No mock defined" }"#),
        )
        .mount(server)
        .await;
}

pub fn read_file(file: &str) -> String {
    fs::read_to_string(file).expect("Could not read file")
}
