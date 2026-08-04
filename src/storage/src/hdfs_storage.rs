use crate::storage::{HDFS_SCHEMA, build_layered_operator};
use datafusion::common::Result;
use opendal::Operator;
use opendal::services::HdfsNativeConfig;

/// HDFS needs no configuration block; the NameNode authority
/// (host:port or HA nameservice) comes from the location itself.
pub fn build_operator(authority: &str) -> Result<Operator> {
    let mut cfg = HdfsNativeConfig::default();
    cfg.name_node = Some(format!("{}://{}", HDFS_SCHEMA, authority));
    build_layered_operator(cfg)
}
