use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use lakelet_storage::storage;
use opendal::Operator;
use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, Mutex};

/// Iceberg storage factory backed by the unified OpenDAL builder in
/// lakelet-storage. It ignores FileIO properties entirely: credentials come
/// from the per-catalog `Storage` config captured at construction time.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct LakeletStorageFactory {
    storage: Option<storage::Storage>,
}

impl LakeletStorageFactory {
    pub(crate) fn new(storage: Option<storage::Storage>) -> Self {
        Self { storage }
    }
}

#[typetag::serde]
impl StorageFactory for LakeletStorageFactory {
    fn build(&self, _config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(LakeletIcebergStorage::new(self.storage.clone())))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct LakeletIcebergStorage {
    storage: Option<storage::Storage>,
    /// Operators cached per `scheme://authority`. Iceberg locations may span
    /// several buckets or NameNodes, so operators are created lazily per
    /// authority instead of being bound to a single one.
    #[serde(skip)]
    operators: Arc<Mutex<HashMap<String, Operator>>>,
}

impl LakeletIcebergStorage {
    fn new(storage: Option<storage::Storage>) -> Self {
        Self {
            storage,
            operators: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Resolve a full location (`scheme://authority/path`) into the operator
    /// serving its authority and the storage-relative path.
    fn resolve(&self, location: &str) -> Result<(Operator, String)> {
        let (scheme, authority) =
            storage::parse_location_schema_authority(location).map_err(|error| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Invalid storage location: {location}"),
                )
                .with_source(error)
            })?;

        let key = format!("{scheme}://{authority}");
        let operator = {
            let mut operators = self.operators.lock().unwrap();
            match operators.get(&key) {
                Some(op) => op.clone(),
                None => {
                    let op = storage::build_operator(&scheme, &authority, self.storage.as_ref())
                        .map_err(|error| {
                            Error::new(
                                ErrorKind::Unexpected,
                                format!("Failed to build operator for {key}"),
                            )
                            .with_source(error)
                        })?
                        .ok_or_else(|| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                format!(
                                    "no storage configured for scheme '{scheme}' of {location}"
                                ),
                            )
                        })?;
                    operators.insert(key.clone(), op.clone());
                    op
                }
            }
        };

        let relative_path = get_relative_path(location, &key)?;
        Ok((operator, relative_path))
    }
}

#[async_trait]
#[typetag::serde]
impl Storage for LakeletIcebergStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let (op, path) = self.resolve(path)?;
        op.exists(&path)
            .await
            .map_err(|error| from_opendal_error("check file existence", error))
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let (op, path) = self.resolve(path)?;
        let meta = op
            .stat(&path)
            .await
            .map_err(|error| from_opendal_error("read file metadata", error))?;
        Ok(FileMetadata {
            size: meta.content_length(),
        })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let (op, path) = self.resolve(path)?;
        Ok(op
            .read(&path)
            .await
            .map_err(|error| from_opendal_error("read file", error))?
            .to_bytes())
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let (op, path) = self.resolve(path)?;
        Ok(Box::new(LakeletFileReader(
            op.reader(&path)
                .await
                .map_err(|error| from_opendal_error("open file", error))?,
        )))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        let (op, path) = self.resolve(path)?;
        op.write(&path, bs)
            .await
            .map_err(|error| from_opendal_error("write file", error))?;
        Ok(())
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let (op, path) = self.resolve(path)?;
        Ok(Box::new(LakeletFileWriter(
            op.writer(&path)
                .await
                .map_err(|error| from_opendal_error("open file for write", error))?,
        )))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let (op, path) = self.resolve(path)?;
        op.delete(&path)
            .await
            .map_err(|error| from_opendal_error("delete file", error))
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let (op, path) = self.resolve(path)?;
        let path = if path.ends_with('/') {
            path
        } else {
            format!("{path}/")
        };
        op.delete_with(&path)
            .recursive(true)
            .await
            .map_err(|error| from_opendal_error("delete prefix", error))
    }

    async fn delete_stream(&self, mut paths: BoxStream<'static, String>) -> Result<()> {
        // Paths may span several authorities; keep one deleter per operator.
        let mut deleters: HashMap<String, opendal::Deleter> = HashMap::new();
        while let Some(location) = paths.next().await {
            let (scheme, authority) =
                storage::parse_location_schema_authority(&location).map_err(|error| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid storage location: {location}"),
                    )
                    .with_source(error)
                })?;
            let key = format!("{scheme}://{authority}");
            let (op, path) = self.resolve(&location)?;
            let deleter = match deleters.entry(key) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => entry.insert(
                    op.deleter()
                        .await
                        .map_err(|error| from_opendal_error("create deleter", error))?,
                ),
            };
            deleter
                .delete(path)
                .await
                .map_err(|error| from_opendal_error("delete file", error))?;
        }
        for (_, mut deleter) in deleters {
            deleter
                .close()
                .await
                .map_err(|error| from_opendal_error("close deleter", error))?;
        }
        Ok(())
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        self.resolve(path)?;
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        self.resolve(path)?;
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

// Newtype wrappers: iceberg's FileRead/FileWrite cannot be implemented
// directly on opendal's Reader/Writer due to orphan rules.
struct LakeletFileReader(opendal::Reader);

#[async_trait]
impl FileRead for LakeletFileReader {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        Ok(self
            .0
            .read(range)
            .await
            .map_err(|error| from_opendal_error("read file range", error))?
            .to_bytes())
    }
}

struct LakeletFileWriter(opendal::Writer);

#[async_trait]
impl FileWrite for LakeletFileWriter {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        self.0
            .write(bs)
            .await
            .map_err(|error| from_opendal_error("write file", error))
    }

    async fn close(&mut self) -> Result<()> {
        let _ = self
            .0
            .close()
            .await
            .map_err(|error| from_opendal_error("close file", error))?;
        Ok(())
    }
}

/// Strip the `scheme://authority/` prefix and percent-decode the rest.
fn get_relative_path(location: &str, prefix: &str) -> Result<String> {
    let rest = location.strip_prefix(prefix).ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Location {location} does not start with {prefix}"),
        )
    })?;

    percent_decode_str(rest.trim_start_matches('/'))
        .decode_utf8()
        .map(|path| path.into_owned())
        .map_err(|error| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Invalid storage path: {location}"),
            )
            .with_source(error)
        })
}

fn from_opendal_error(operation: &str, error: opendal::Error) -> Error {
    Error::new(ErrorKind::Unexpected, format!("Failed to {operation}")).with_source(error)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolve_relative(location: &str, prefix: &str) -> String {
        get_relative_path(location, prefix).unwrap()
    }

    #[test]
    fn test_relative_path_hdfs() {
        assert_eq!(
            resolve_relative(
                "hdfs://namenode:8020/warehouse/db/table/metadata.json",
                "hdfs://namenode:8020"
            ),
            "warehouse/db/table/metadata.json"
        );
    }

    #[test]
    fn test_relative_path_s3() {
        assert_eq!(
            resolve_relative("s3://bucket/warehouse/db/table/data.parquet", "s3://bucket"),
            "warehouse/db/table/data.parquet"
        );
    }

    #[test]
    fn test_relative_path_decodes_url_encoding() {
        assert_eq!(
            resolve_relative(
                "hdfs://namenode:8020/warehouse/table%20name/metadata.json",
                "hdfs://namenode:8020"
            ),
            "warehouse/table name/metadata.json"
        );
    }

    #[test]
    fn test_relative_path_rejects_prefix_mismatch() {
        let error =
            get_relative_path("s3://other/warehouse/data.parquet", "s3://bucket").unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn test_resolve_requires_storage_config() {
        let storage = LakeletIcebergStorage::new(None);
        let error = storage
            .resolve("s3://bucket/warehouse/metadata.json")
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(error.to_string().contains("no storage configured"));
    }

    #[test]
    fn test_resolve_hdfs_needs_no_config_and_caches_operator() {
        let storage = LakeletIcebergStorage::new(None);
        let (op, path) = storage
            .resolve("hdfs://namenode:8020/warehouse/db/t/metadata.json")
            .unwrap();
        assert_eq!(op.info().scheme(), "hdfs-native");
        assert_eq!(path, "warehouse/db/t/metadata.json");
        assert_eq!(storage.operators.lock().unwrap().len(), 1);

        // A second authority gets its own operator.
        storage
            .resolve("hdfs://other:8020/warehouse/db/t/metadata.json")
            .unwrap();
        assert_eq!(storage.operators.lock().unwrap().len(), 2);
    }

    #[test]
    fn test_resolve_unsupported_scheme_errors() {
        let storage = LakeletIcebergStorage::new(None);
        let error = storage
            .resolve("gcs://bucket/warehouse/metadata.json")
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unexpected);
    }
}
