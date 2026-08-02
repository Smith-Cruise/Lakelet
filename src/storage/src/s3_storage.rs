use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Storage {
    #[serde(rename = "region")]
    pub region: Option<String>,
    #[serde(rename = "endpoint")]
    pub endpoint: Option<String>,
    #[serde(rename = "access-key")]
    pub access_key: Option<String>,
    #[serde(rename = "secret-key")]
    pub secret_key: Option<String>,
    #[serde(rename = "path-style-access", default)]
    pub path_style_access: bool,
}
