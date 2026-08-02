use crate::operator::build_root_object_store;
use crate::oss_storage::OSSStorage;
use crate::s3_storage::S3Storage;
use datafusion::catalog::Session;
use datafusion::common::DataFusionError;
use datafusion::common::Result;
use iceberg::io::{
    OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET, OSS_ENDPOINT, S3_ACCESS_KEY_ID, S3_ENDPOINT,
    S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use url::Url;

pub const S3_SCHEMA: &str = "s3";
pub const S3A_SCHEMA: &str = "s3a";
pub const OSS_SCHEMA: &str = "oss";
pub const HDFS_SCHEMA: &str = "hdfs";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Storage {
    #[serde(rename = "s3-storage")]
    s3_storage: Option<S3Storage>,
    #[serde(rename = "oss-storage")]
    oss_storage: Option<OSSStorage>,
}

impl Storage {
    pub fn s3_storage(&self) -> Option<&S3Storage> {
        self.s3_storage.as_ref()
    }

    pub fn oss_storage(&self) -> Option<&OSSStorage> {
        self.oss_storage.as_ref()
    }

    pub fn build_paimon_file_io_properties(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        if let Some(s3_storage) = &self.s3_storage {
            if let Some(region) = &s3_storage.region {
                map.insert("s3.region".to_string(), region.clone());
            } else {
                map.insert("s3.region".to_string(), "us-east-1".to_string());
            }
            if let Some(endpoint) = &s3_storage.endpoint {
                map.insert("s3.endpoint".to_string(), endpoint.clone());
            }
            if let Some(access_key) = &s3_storage.access_key {
                map.insert("s3.access-key".to_string(), access_key.clone());
            }
            if let Some(secret_key) = &s3_storage.secret_key {
                map.insert("s3.secret-key".to_string(), secret_key.clone());
            }
            map.insert(
                "s3.path-style-access".to_string(),
                s3_storage.path_style_access.to_string(),
            );
        }

        if let Some(oss_storage) = &self.oss_storage {
            if let Some(endpoint) = &oss_storage.endpoint {
                map.insert("fs.oss.endpoint".to_string(), endpoint.clone());
            }
            if let Some(access_key) = &oss_storage.access_key {
                map.insert("fs.oss.accessKeyId".to_string(), access_key.clone());
            }
            if let Some(secret_key) = &oss_storage.secret_key {
                map.insert("fs.oss.accessKeySecret".to_string(), secret_key.clone());
            }
        }
        map
    }

    pub fn build_iceberg_file_io_properties(&self) -> HashMap<String, String> {
        let mut map: HashMap<String, String> = HashMap::new();
        if let Some(s3_storage) = &self.s3_storage {
            if let Some(region) = &s3_storage.region {
                map.insert(S3_REGION.into(), region.clone());
            } else {
                map.insert(S3_REGION.into(), "us-east-1".to_string());
            }
            if let Some(endpoint) = &s3_storage.endpoint {
                map.insert(S3_ENDPOINT.into(), endpoint.clone());
            }
            if let Some(access_key) = &s3_storage.access_key {
                map.insert(S3_ACCESS_KEY_ID.into(), access_key.clone());
            }
            if let Some(secret_key) = &s3_storage.secret_key {
                map.insert(S3_SECRET_ACCESS_KEY.into(), secret_key.clone());
            }
            map.insert(
                S3_PATH_STYLE_ACCESS.into(),
                s3_storage.path_style_access.to_string(),
            );
        }

        if let Some(oss_storage) = &self.oss_storage {
            if let Some(endpoint) = &oss_storage.endpoint {
                map.insert(OSS_ENDPOINT.into(), endpoint.clone());
            }
            if let Some(access_key) = &oss_storage.access_key {
                map.insert(OSS_ACCESS_KEY_ID.into(), access_key.clone());
            }
            if let Some(secret_key) = &oss_storage.secret_key {
                map.insert(OSS_ACCESS_KEY_SECRET.into(), secret_key.clone());
            }
        }
        map
    }
}

pub fn try_register_storage_info_session(
    storage: Option<&Storage>,
    table_location: impl Into<String>,
    session: &dyn Session,
) -> Result<()> {
    let table_location = table_location.into();
    let (path_schema, path_bucket) = parse_location_schema_authority(&table_location)?;

    let object_store_path = Url::parse(&format!("{}://{}", path_schema, path_bucket))
        .map_err(|e| DataFusionError::External(e.into()))?;

    let registry = &session.runtime_env().object_store_registry;
    if registry.get_store(&object_store_path).is_ok() {
        return Ok(());
    }

    if let Some(store) = build_root_object_store(&path_schema, &path_bucket, storage)? {
        registry.register_store(&object_store_path, store);
    }
    Ok(())
}

pub fn parse_location_schema_authority(path: &str) -> Result<(String, String)> {
    let parsed_url = Url::parse(path).map_err(|e| DataFusionError::External(e.into()))?;
    let url_schema = parsed_url.scheme();
    // Use the full authority (host:port) so HDFS NameNode ports are preserved.
    // For s3/oss locations without a port, this is identical to the host.
    let authority = parsed_url.authority();
    if authority.is_empty() {
        return Err(DataFusionError::Internal(
            "failed to parse authority".into(),
        ));
    }
    Ok((url_schema.to_string(), authority.to_string()))
}

#[cfg(test)]
mod tests {
    use crate::storage::{
        Storage, parse_location_schema_authority, try_register_storage_info_session,
    };
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::prelude::SessionContext;
    use iceberg::io::{S3_PATH_STYLE_ACCESS, S3_REGION};

    #[test]
    fn test_parse_storage() {
        let text = r#"
            s3-storage = { endpoint = "http://127.0.0.1:9000", region = "us-east-1", access-key = "admin", secret-key = "password", path-style-access = true }
        "#;

        let storages: Storage = toml::from_str(text).unwrap();
        assert!(storages.s3_storage.is_some());
        assert!(storages.oss_storage.is_none());
        let s3_storage = storages.s3_storage.unwrap();
        assert_eq!("http://127.0.0.1:9000", &s3_storage.endpoint.unwrap());
        assert_eq!("us-east-1", &s3_storage.region.unwrap());
        assert_eq!("admin", &s3_storage.access_key.unwrap());
        assert_eq!("password", &s3_storage.secret_key.unwrap());
        assert!(s3_storage.path_style_access);

        let text = r#"
            s3-storage = { endpoint = "http://127.0.0.1:9000", region = "us-east-1", access-key = "admin", secret-key = "password" }
            oss-storage = { endpoint = "http://127.0.0.1:9000", access-key = "admin", secret-key = "password", path-style-access = false }
        "#;
        let storage: Storage = toml::from_str(text).unwrap();
        assert!(storage.s3_storage.is_some());
        assert!(storage.oss_storage.is_some());
        let s3_storage = storage.s3_storage.unwrap();
        assert!(!s3_storage.path_style_access);
        let oss_storage = storage.oss_storage.unwrap();
        assert_eq!("http://127.0.0.1:9000", &oss_storage.endpoint.unwrap());
        assert_eq!("admin", &oss_storage.access_key.unwrap());
        assert_eq!("password", &oss_storage.secret_key.unwrap());
        assert!(!oss_storage.path_style_access);
    }

    #[test]
    fn test_build_iceberg_file_io_properties_includes_s3_path_style_access() {
        let text = r#"
            s3-storage = { endpoint = "http://127.0.0.1:9000", region = "us-east-1", access-key = "admin", secret-key = "password", path-style-access = true }
        "#;

        let storage: Storage = toml::from_str(text).unwrap();
        let properties = storage.build_iceberg_file_io_properties();

        assert_eq!(
            properties.get(S3_PATH_STYLE_ACCESS).map(String::as_str),
            Some("true")
        );

        let text = r#"
            s3-storage = { endpoint = "http://127.0.0.1:9000", region = "us-east-1", access-key = "admin", secret-key = "password" }
        "#;

        let storage: Storage = toml::from_str(text).unwrap();
        let properties = storage.build_iceberg_file_io_properties();

        assert_eq!(
            properties.get(S3_PATH_STYLE_ACCESS).map(String::as_str),
            Some("false")
        );
    }

    #[test]
    fn test_build_iceberg_file_io_properties_defaults_s3_region() {
        let text = r#"
            s3-storage = { endpoint = "http://127.0.0.1:9000", access-key = "admin", secret-key = "password" }
        "#;

        let storage: Storage = toml::from_str(text).unwrap();
        let properties = storage.build_iceberg_file_io_properties();

        assert_eq!(
            properties.get(S3_REGION).map(String::as_str),
            Some("us-east-1")
        );
    }

    #[test]
    fn test_build_paimon_file_io_properties() {
        let text = r#"
            s3-storage = { endpoint = "http://127.0.0.1:9000", region = "us-east-1", access-key = "admin", secret-key = "password", path-style-access = true }
            oss-storage = { endpoint = "https://oss.example.com", access-key = "oss-ak", secret-key = "oss-sk" }
        "#;

        let storage: Storage = toml::from_str(text).unwrap();
        let properties = storage.build_paimon_file_io_properties();

        assert_eq!(properties["s3.endpoint"], "http://127.0.0.1:9000");
        assert_eq!(properties["s3.region"], "us-east-1");
        assert_eq!(properties["s3.access-key"], "admin");
        assert_eq!(properties["s3.secret-key"], "password");
        assert_eq!(properties["s3.path-style-access"], "true");
        assert_eq!(properties["fs.oss.endpoint"], "https://oss.example.com");
        assert_eq!(properties["fs.oss.accessKeyId"], "oss-ak");
        assert_eq!(properties["fs.oss.accessKeySecret"], "oss-sk");
    }

    #[test]
    fn test_parse_location_schema_authority() {
        let (schema, bucket) =
            parse_location_schema_authority("s3://bucket/tests/testdata/schema.json").unwrap();
        assert_eq!("s3", schema);
        assert_eq!("bucket", bucket);

        let (schema, bucket) =
            parse_location_schema_authority("hdfs://namenode:8020/user/hive/warehouse/db/t")
                .unwrap();
        assert_eq!("hdfs", schema);
        assert_eq!("namenode:8020", bucket);
    }

    #[test]
    fn test_register_hdfs_without_storage_config() {
        let ctx = SessionContext::new();

        try_register_storage_info_session(
            None,
            "hdfs://namenode:8020/user/hive/warehouse/db/t",
            &ctx.state(),
        )
        .unwrap();

        assert!(
            ctx.runtime_env()
                .object_store(ObjectStoreUrl::parse("hdfs://namenode:8020").unwrap())
                .is_ok()
        );
    }
}
