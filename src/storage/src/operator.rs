use crate::storage::{HDFS_SCHEMA, OSS_SCHEMA, S3_SCHEMA, S3A_SCHEMA, Storage};
use datafusion::common::{DataFusionError, Result};
use datafusion::object_store::ObjectStore;
use object_store_opendal::OpendalStore;
use opendal::Operator;
use opendal::layers::{RetryLayer, TimeoutLayer};
use opendal::services::{HdfsNativeConfig, OssConfig, S3Config};
use std::sync::Arc;

/// Build an OpenDAL operator rooted at the storage's top level (bucket root
/// for s3/oss, `/` for hdfs). This is the single place where a location
/// scheme + authority is mapped onto a storage backend and credentials.
///
/// Returns `Ok(None)` when the scheme is unknown or the catalog has no
/// storage configuration for it; callers decide whether that is an error.
pub fn build_operator(
    scheme: &str,
    authority: &str,
    storage: Option<&Storage>,
) -> Result<Option<Operator>> {
    let operator = match scheme {
        S3_SCHEMA | S3A_SCHEMA => match storage.and_then(|storage| storage.s3_storage()) {
            Some(s3_storage) => {
                let mut cfg = S3Config::default();
                cfg.bucket = authority.to_string();
                // When unset, OpenDAL falls back to AWS_REGION/AWS_DEFAULT_REGION
                // and errors with a clear message if those are missing too.
                cfg.region = s3_storage.region.clone();
                cfg.endpoint = s3_storage.endpoint.clone();
                cfg.access_key_id = s3_storage.access_key.clone();
                cfg.secret_access_key = s3_storage.secret_key.clone();
                // OpenDAL defaults to path-style, the inverse of our config default.
                cfg.enable_virtual_host_style = !s3_storage.path_style_access;
                Some(build_from_config(cfg)?)
            }
            None => None,
        },
        OSS_SCHEMA => match storage.and_then(|storage| storage.oss_storage()) {
            Some(oss_storage) => {
                let mut cfg = OssConfig::default();
                cfg.bucket = authority.to_string();
                // The endpoint must not contain the bucket name; OpenDAL
                // prepends `{bucket}.` itself in virtual-hosted style.
                cfg.endpoint = oss_storage.endpoint.clone();
                cfg.access_key_id = oss_storage.access_key.clone();
                cfg.access_key_secret = oss_storage.secret_key.clone();
                cfg.addressing_style = Some(
                    if oss_storage.path_style_access {
                        "path"
                    } else {
                        "virtual"
                    }
                    .to_string(),
                );
                Some(build_from_config(cfg)?)
            }
            None => None,
        },
        HDFS_SCHEMA => {
            // HDFS needs no configuration block; the NameNode authority
            // (host:port or HA nameservice) comes from the location itself.
            let mut cfg = HdfsNativeConfig::default();
            cfg.name_node = Some(format!("{}://{}", HDFS_SCHEMA, authority));
            Some(build_from_config(cfg)?)
        }
        _ => None,
    };
    Ok(operator)
}

/// Build a root-level `ObjectStore` for DataFusion's registry (and Delta's
/// storage backend) on top of the unified OpenDAL operator.
pub fn build_root_object_store(
    scheme: &str,
    authority: &str,
    storage: Option<&Storage>,
) -> Result<Option<Arc<dyn ObjectStore>>> {
    Ok(build_operator(scheme, authority, storage)?
        .map(|op| Arc::new(OpendalStore::new(op)) as Arc<dyn ObjectStore>))
}

fn build_from_config<C: opendal::Configurator>(cfg: C) -> Result<Operator> {
    // TimeoutLayer must sit inside RetryLayer so each retry attempt gets an
    // independent timeout; a hung request would otherwise never be retried.
    // RetryLayer is what turns transient HTTP failures (e.g. a stale pooled
    // connection closed by the server) into silent retries instead of query
    // errors.
    Ok(Operator::from_config(cfg)
        .map_err(|err| DataFusionError::External(Box::new(err)))?
        .layer(TimeoutLayer::new())
        .layer(RetryLayer::new())
        .finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_storage(text: &str) -> Storage {
        toml::from_str(text).unwrap()
    }

    const S3_TOML: &str = r#"
        s3-storage = { endpoint = "http://127.0.0.1:9000", region = "cn-north-1", access-key = "ak", secret-key = "sk", path-style-access = true }
    "#;

    const OSS_TOML: &str = r#"
        oss-storage = { endpoint = "https://oss-cn-hangzhou.aliyuncs.com", access-key = "ak", secret-key = "sk" }
    "#;

    #[test]
    fn test_build_operator_s3() {
        let storage = parse_storage(S3_TOML);
        for scheme in [S3_SCHEMA, S3A_SCHEMA] {
            let op = build_operator(scheme, "bucket", Some(&storage))
                .unwrap()
                .expect("s3 operator should be built");
            assert_eq!(op.info().name(), "bucket");
            assert_eq!(op.info().scheme(), "s3");
        }
    }

    #[test]
    fn test_build_operator_oss() {
        let storage = parse_storage(OSS_TOML);
        let op = build_operator(OSS_SCHEMA, "bucket", Some(&storage))
            .unwrap()
            .expect("oss operator should be built");
        assert_eq!(op.info().name(), "bucket");
        assert_eq!(op.info().scheme(), "oss");
    }

    #[test]
    fn test_build_operator_hdfs_needs_no_config() {
        let op = build_operator(HDFS_SCHEMA, "namenode:8020", None)
            .unwrap()
            .expect("hdfs operator should be built without storage config");
        assert_eq!(op.info().scheme(), "hdfs-native");
    }

    #[test]
    fn test_build_operator_missing_storage_config() {
        assert!(build_operator(S3_SCHEMA, "bucket", None).unwrap().is_none());
        let storage = parse_storage(S3_TOML);
        assert!(
            build_operator(OSS_SCHEMA, "bucket", Some(&storage))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_build_operator_unknown_scheme() {
        let storage = parse_storage(S3_TOML);
        assert!(
            build_operator("gcs", "bucket", Some(&storage))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_build_root_object_store() {
        let storage = parse_storage(S3_TOML);
        assert!(
            build_root_object_store(S3_SCHEMA, "bucket", Some(&storage))
                .unwrap()
                .is_some()
        );
        assert!(
            build_root_object_store("gcs", "bucket", Some(&storage))
                .unwrap()
                .is_none()
        );
    }
}
