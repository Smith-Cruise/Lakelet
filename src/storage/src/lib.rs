// OpenDAL's layered operators (retry/timeout over the service backends)
// produce deeply nested generic types that exceed the default limit.
#![recursion_limit = "256"]

pub mod hdfs_storage;
pub mod oss_storage;
pub mod s3_storage;
pub mod storage;
