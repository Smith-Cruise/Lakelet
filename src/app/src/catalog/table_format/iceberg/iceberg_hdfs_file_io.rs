use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use lakelet_storage::hdfs_storage;
use lakelet_storage::storage::HDFS_SCHEMA;
use opendal::Operator;
use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};
use std::ops::Range;
use std::sync::{Arc, OnceLock};
use url::Url;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct HdfsStorageFactory {
    authority: String,
}

impl HdfsStorageFactory {
    pub(crate) fn try_new(location: &str) -> Result<Self> {
        let parsed = parse_hdfs_url(location)?;
        Ok(Self {
            authority: parsed.authority().to_string(),
        })
    }
}

#[typetag::serde]
impl StorageFactory for HdfsStorageFactory {
    fn build(&self, _config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(HdfsStorage::new(self.authority.clone())))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HdfsStorage {
    authority: String,
    #[serde(skip)]
    operator: Arc<OnceLock<Operator>>,
}

impl HdfsStorage {
    fn new(authority: String) -> Self {
        Self {
            authority,
            operator: Arc::new(OnceLock::new()),
        }
    }

    fn operator(&self) -> Result<Operator> {
        if let Some(op) = self.operator.get() {
            return Ok(op.clone());
        }

        // Reuse the shared OpenDAL builder so HDFS access shares one
        // configuration (and retry/timeout layers) across all table formats.
        let op = hdfs_storage::build_operator(&self.authority).map_err(|error| {
            Error::new(ErrorKind::Unexpected, "Failed to build HDFS operator").with_source(error)
        })?;
        let _ = self.operator.set(op.clone());
        Ok(op)
    }

    fn get_relative_path(&self, location: &str) -> Result<String> {
        let parsed = parse_hdfs_url(location)?;
        if parsed.authority() != self.authority {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "HDFS authority mismatch: expected {}, got {}",
                    self.authority,
                    parsed.authority()
                ),
            ));
        }

        percent_decode_str(parsed.path().trim_start_matches('/'))
            .decode_utf8()
            .map(|path| path.into_owned())
            .map_err(|error| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Invalid HDFS path: {location}"),
                )
                .with_source(error)
            })
    }
}

#[async_trait]
#[typetag::serde]
impl Storage for HdfsStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let path = self.get_relative_path(path)?;
        self.operator()?
            .exists(&path)
            .await
            .map_err(|error| from_opendal_error("check HDFS file existence", error))
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let path = self.get_relative_path(path)?;
        let meta = self
            .operator()?
            .stat(&path)
            .await
            .map_err(|error| from_opendal_error("read HDFS file metadata", error))?;
        Ok(FileMetadata {
            size: meta.content_length(),
        })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let path = self.get_relative_path(path)?;
        Ok(self
            .operator()?
            .read(&path)
            .await
            .map_err(|error| from_opendal_error("read HDFS file", error))?
            .to_bytes())
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let path = self.get_relative_path(path)?;
        Ok(Box::new(HdfsFileReader(
            self.operator()?
                .reader(&path)
                .await
                .map_err(|error| from_opendal_error("open HDFS file", error))?,
        )))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        let path = self.get_relative_path(path)?;
        self.operator()?
            .write(&path, bs)
            .await
            .map_err(|error| from_opendal_error("write HDFS file", error))?;
        Ok(())
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let path = self.get_relative_path(path)?;
        Ok(Box::new(HdfsFileWriter(
            self.operator()?
                .writer(&path)
                .await
                .map_err(|error| from_opendal_error("open HDFS file for write", error))?,
        )))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let path = self.get_relative_path(path)?;
        self.operator()?
            .delete(&path)
            .await
            .map_err(|error| from_opendal_error("delete HDFS file", error))
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let path = self.get_relative_path(path)?;
        let path = if path.ends_with('/') {
            path
        } else {
            format!("{path}/")
        };
        self.operator()?
            .delete_with(&path)
            .recursive(true)
            .await
            .map_err(|error| from_opendal_error("delete HDFS prefix", error))
    }

    async fn delete_stream(&self, mut paths: BoxStream<'static, String>) -> Result<()> {
        let mut deleter = self
            .operator()?
            .deleter()
            .await
            .map_err(|error| from_opendal_error("create HDFS deleter", error))?;
        while let Some(path) = paths.next().await {
            let path = self.get_relative_path(&path)?;
            deleter
                .delete(path)
                .await
                .map_err(|error| from_opendal_error("delete HDFS file", error))?;
        }
        deleter
            .close()
            .await
            .map_err(|error| from_opendal_error("close HDFS deleter", error))?;
        Ok(())
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        self.get_relative_path(path)?;
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        self.get_relative_path(path)?;
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

// Newtype wrappers: iceberg's FileRead/FileWrite cannot be implemented
// directly on opendal's Reader/Writer due to orphan rules.
struct HdfsFileReader(opendal::Reader);

#[async_trait]
impl FileRead for HdfsFileReader {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        Ok(self
            .0
            .read(range)
            .await
            .map_err(|error| from_opendal_error("read HDFS file range", error))?
            .to_bytes())
    }
}

struct HdfsFileWriter(opendal::Writer);

#[async_trait]
impl FileWrite for HdfsFileWriter {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        self.0
            .write(bs)
            .await
            .map_err(|error| from_opendal_error("write HDFS file", error))
    }

    async fn close(&mut self) -> Result<()> {
        let _ = self
            .0
            .close()
            .await
            .map_err(|error| from_opendal_error("close HDFS file", error))?;
        Ok(())
    }
}

fn parse_hdfs_url(location: &str) -> Result<Url> {
    let parsed = Url::parse(location).map_err(|error| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid HDFS location: {location}"),
        )
        .with_source(error)
    })?;

    if parsed.scheme() != HDFS_SCHEMA {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid HDFS scheme: {}", parsed.scheme()),
        ));
    }
    if parsed.authority().is_empty() {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!("HDFS location has no authority: {location}"),
        ));
    }

    Ok(parsed)
}

fn from_opendal_error(operation: &str, error: opendal::Error) -> Error {
    Error::new(ErrorKind::Unexpected, format!("Failed to {operation}")).with_source(error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hdfs_factory_preserves_authority_port() {
        let factory =
            HdfsStorageFactory::try_new("hdfs://namenode:8020/warehouse/db/table/metadata.json")
                .unwrap();
        assert_eq!(factory.authority, "namenode:8020");
    }

    #[test]
    fn test_relative_path() {
        let storage = HdfsStorage::new("namenode:8020".to_string());

        assert_eq!(
            storage
                .get_relative_path("hdfs://namenode:8020/warehouse/db/table/metadata.json")
                .unwrap(),
            "warehouse/db/table/metadata.json"
        );
    }

    #[test]
    fn test_relative_path_decodes_url_encoding() {
        let storage = HdfsStorage::new("namenode:8020".to_string());

        assert_eq!(
            storage
                .get_relative_path("hdfs://namenode:8020/warehouse/table%20name/metadata.json")
                .unwrap(),
            "warehouse/table name/metadata.json"
        );
    }

    #[test]
    fn test_rejects_different_authority() {
        let storage = HdfsStorage::new("namenode:8020".to_string());

        let error = storage
            .get_relative_path("hdfs://other:8020/warehouse/metadata.json")
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn test_rejects_non_hdfs_location() {
        let error = HdfsStorageFactory::try_new("s3://bucket/metadata.json").unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn test_new_output_is_supported() {
        let storage = HdfsStorage::new("namenode:8020".to_string());
        assert!(
            storage
                .new_output("hdfs://namenode:8020/warehouse/metadata.json")
                .is_ok()
        );
    }
}
