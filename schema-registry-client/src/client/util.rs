use std::collections::HashMap;
use std::fmt;
use std::io::Write;
use std::str::FromStr;

use crate::client::util;
use base64::prelude::BASE64_STANDARD;
use base64::write::EncoderWriter;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{header, Client, Proxy};

use crate::config::{Authentication, SchemaRegistryConfig};
use crate::error::ConfigurationError;

pub fn build_auth_headers(
    auth: &Authentication,
) -> Result<(HeaderName, HeaderValue), ConfigurationError> {
    match auth {
        Authentication::Bearer { token } => bearer_auth(token),
        Authentication::Basic { username, password } => basic_auth(username, password.as_ref()),
    }
}

pub fn bearer_auth(token: &str) -> Result<(HeaderName, HeaderValue), ConfigurationError> {
    let header_name = HeaderName::from(header::AUTHORIZATION);
    let mut header = HeaderValue::from_str(&format!("Bearer {}", token))?;
    header.set_sensitive(true);
    Ok((header_name, header))
}

pub fn basic_auth<U, P>(
    username: U,
    password: Option<P>,
) -> Result<(HeaderName, HeaderValue), ConfigurationError>
where
    U: fmt::Display,
    P: fmt::Display,
{
    let mut buf = b"Basic ".to_vec();
    {
        let mut encoder = EncoderWriter::new(&mut buf, &BASE64_STANDARD);

        write!(encoder, "{}:", username)?;
        if let Some(password) = password {
            write!(encoder, "{}", password)?;
        }
    }

    let header_name = HeaderName::from(header::AUTHORIZATION);
    let mut header_value = HeaderValue::from_bytes(&buf)?;
    header_value.set_sensitive(true);
    Ok((header_name, header_value))
}

pub fn build_headers(headers: &HashMap<String, String>) -> Result<HeaderMap, ConfigurationError> {
    let mut header_map = HeaderMap::new();

    for (name, value) in headers {
        let header_name = HeaderName::from_str(name)?;
        let header_value = HeaderValue::from_str(value)?;
        header_map.insert(header_name, header_value);
    }

    Ok(header_map)
}

pub fn build_proxy(proxy: &String) -> Result<Proxy, ConfigurationError> {
    let proxy = Proxy::all(proxy)?;
    Ok(proxy)
}

pub fn build_http_client(conf: &SchemaRegistryConfig) -> Result<Client, ConfigurationError> {
    let mut default_headers = HeaderMap::new();

    if let Some(headers) = &conf.headers {
        default_headers = build_headers(headers)?;
    }

    if let Some(auth) = &conf.authentication {
        let (header_name, header_value) = build_auth_headers(&auth)?;
        default_headers.insert(header_name, header_value);
    }

    let proxy = conf
        .proxy
        .as_ref()
        .map(|proxy| build_proxy(&proxy))
        .transpose()?;

    let mut client_builder = Client::builder().default_headers(default_headers);

    if let Some(proxy) = proxy {
        client_builder = client_builder.proxy(proxy);
    }

    let http_client = client_builder.build().map_err(ConfigurationError::from)?;

    Ok(http_client)
}
