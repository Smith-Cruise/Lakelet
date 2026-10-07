use datafusion::catalog::Session;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::cache::TableScopedPath;
use datafusion::execution::cache::cache_manager::CachedFileList;
use datafusion::object_store::path::Path;
use datafusion::object_store::{ObjectMeta, ObjectStore};
use futures::StreamExt;
use std::sync::Arc;
use url::Url;

/// Lists files from all directories and returns them as one flattened collection.
pub(super) async fn list_files_by_directories(
    state: &dyn Session,
    object_store: &Arc<dyn ObjectStore>,
    dir_locations: Vec<String>,
) -> Result<Vec<ObjectMeta>> {
    Ok(
        list_files_by_directories_grouped(state, object_store, dir_locations)
            .await?
            .into_iter()
            .flatten()
            .collect(),
    )
}

/// Lists files per directory, preserving the input directory order in the outer collection.
pub(super) async fn list_files_by_directories_grouped(
    state: &dyn Session,
    object_store: &Arc<dyn ObjectStore>,
    dir_locations: Vec<String>,
) -> Result<Vec<Vec<ObjectMeta>>> {
    let concurrency = state.config_options().execution.meta_fetch_concurrency;
    let tasks = dir_locations.into_iter().map(|location| {
        let object_store = Arc::clone(object_store);

        async move { list_files(state, &object_store, &location).await }
    });

    futures::stream::iter(tasks)
        .buffered(concurrency)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect()
}

/// Recursively lists the visible, non-empty data files under a table or
/// partition directory.
///
/// `object_store` is the store registered for the location's scheme and
/// authority (e.g. `s3://bucket`), so every path it takes and returns is
/// relative to that root, without a leading `/`.
pub(super) async fn list_files(
    state: &dyn Session,
    object_store: &Arc<dyn ObjectStore>,
    // The full table or partition location from the metastore, e.g.
    // `s3://bucket/warehouse/db.db/t/dt=2024-01-01` or
    // `hdfs://namenode:8020/user/hive/warehouse/db.db/t`.
    directory_full_location: &str,
) -> Result<Vec<ObjectMeta>> {
    // The same directory relative to the object store root, e.g.
    // `warehouse/db.db/t/dt=2024-01-01` or `user/hive/warehouse/db.db/t`.
    let relative_path = location_to_object_store_path(directory_full_location)?;
    let cache_key = TableScopedPath {
        table: None,
        path: relative_path.clone(),
    };

    let list_files_cache = state.runtime_env().cache_manager.get_list_files_cache();
    if let Some(cache) = &list_files_cache
        && let Some(cached_files) = cache.get(&cache_key)
    {
        return Ok(cached_files.files.as_ref().clone());
    }

    let file_object_metas: Vec<_> = object_store
        .list(Some(&relative_path))
        .collect::<Vec<_>>()
        .await;

    let mut results = Vec::new();
    for file_object_meta in file_object_metas {
        let meta = file_object_meta.map_err(|e| DataFusionError::External(Box::new(e)))?;
        // `meta.location` is the file path relative to the object store root,
        // including files in subdirectories, e.g.
        // `warehouse/db.db/t/dt=2024-01-01/000000_0` or
        // `warehouse/db.db/t/dt=2024-01-01/_temporary/0/part-00000`.
        if meta.size == 0 || is_hidden_file(&meta.location, &relative_path) {
            continue;
        }
        results.push(meta);
    }

    if let Some(cache) = list_files_cache {
        cache.put(&cache_key, CachedFileList::new(results.clone()));
    }

    Ok(results)
}

/// Hive skips files whose name, or any directory between the listed root and
/// the file, starts with `_` or `.` (e.g. `_SUCCESS`, `_temporary/`,
/// `.hive-staging_*/`).
///
/// Both paths are relative to the object store root, e.g. `location` is
/// `warehouse/db.db/t/_temporary/0/part-00000` and `root` is
/// `warehouse/db.db/t`. Only the segments below `root` (`_temporary`, `0`,
/// `part-00000`) are checked, so a table stored under a directory like
/// `_warehouse/` is not hidden as a whole.
fn is_hidden_file(location: &Path, root: &Path) -> bool {
    match location.prefix_match(root) {
        Some(mut parts) => parts.any(|part| {
            let part = part.as_ref();
            part.starts_with('_') || part.starts_with('.')
        }),
        None => location
            .filename()
            .is_some_and(|name| name.starts_with('_') || name.starts_with('.')),
    }
}

/// Strips the scheme and authority from a full location, e.g.
/// `s3://bucket/warehouse/db.db/t` becomes `warehouse/db.db/t`.
fn location_to_object_store_path(location: &str) -> Result<Path> {
    Url::parse(location).map_err(|e| DataFusionError::External(e.into()))?;
    let (_, authority_and_path) = location.split_once("://").ok_or_else(|| {
        DataFusionError::Plan(format!(
            "Expected a fully qualified Hive location: {location}"
        ))
    })?;
    // Hive locations contain literal object keys. OpendalStore decodes Path
    // once, so encode the raw key rather than URL's normalized path.
    let path = authority_and_path
        .split_once('/')
        .map_or("", |(_, path)| path);
    Ok(Path::from(path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::object_store::ObjectStoreExt;
    use datafusion::object_store::memory::InMemory;
    use datafusion::object_store::path::Path;
    use datafusion::prelude::SessionContext;

    #[test]
    fn test_location_to_object_store_path() {
        for (location, expected) in [
            (
                "s3://warehouse/hive/tpch_hive.db/textfile_no_partition_table",
                "hive/tpch_hive.db/textfile_no_partition_table",
            ),
            (
                "s3://warehouse/hive/tpch_hive.db/textfile_partition_table/p=1",
                "hive/tpch_hive.db/textfile_partition_table/p=1",
            ),
            (
                "s3://warehouse/hive/table/region=中文%2F%3D%25",
                "hive/table/region=中文%2F%3D%25",
            ),
            (
                "s3://warehouse/hive/table/region=50%/value=%252F",
                "hive/table/region=50%/value=%252F",
            ),
            (
                "hdfs://namenode:8020/user/hive/warehouse/region=中文%25",
                "user/hive/warehouse/region=中文%25",
            ),
            ("s3://warehouse/", ""),
        ] {
            let path = location_to_object_store_path(location).unwrap();
            // Match the object key OpendalStore receives after decoding once.
            let raw_path = percent_encoding::percent_decode_str(path.as_ref())
                .decode_utf8()
                .unwrap();
            assert_eq!(raw_path, expected, "{location}");
        }
        assert!(location_to_object_store_path("not a location").is_err());
    }

    #[tokio::test]
    async fn test_list_files_filters_hidden_and_empty_objects() {
        let ctx = SessionContext::new();
        let state = ctx.state();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        for root in [
            "table/dt=2024-01-01",
            "table/region=中文%2F%3D%25",
            "table/region=50%",
        ] {
            for (name, data) in [
                ("file1.parquet", b"a".as_slice()),
                ("_temporary", b"a".as_slice()),
                (".metadata", b"a".as_slice()),
                ("empty.parquet", b"".as_slice()),
                ("_temporary/0/part-0", b"a".as_slice()),
                (".hive-staging_1/-ext-1/000000_0", b"a".as_slice()),
                ("HIVE_UNION_SUBDIR_1/000000_0", b"a".as_slice()),
            ] {
                put_test_object(&store, &format!("{root}/{name}"), data).await;
            }
            let files = list_files(&state, &store, &format!("memory:///{root}"))
                .await
                .unwrap();

            let mut paths: Vec<String> = files
                .iter()
                .map(|file| {
                    percent_encoding::percent_decode_str(file.location.as_ref())
                        .decode_utf8()
                        .unwrap()
                        .into_owned()
                })
                .collect();
            paths.sort();
            assert_eq!(
                paths,
                vec![
                    format!("{root}/HIVE_UNION_SUBDIR_1/000000_0"),
                    format!("{root}/file1.parquet"),
                ]
            );
        }
    }

    #[tokio::test]
    async fn test_list_files_under_hidden_table_root() {
        // Only the part below the listed root is checked.
        let ctx = SessionContext::new();
        let state = ctx.state();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        put_test_object(&store, "_warehouse/table/file1.parquet", b"a").await;

        let files = list_files(&state, &store, "memory:///_warehouse/table")
            .await
            .unwrap();
        assert_eq!(files.len(), 1);
    }

    #[tokio::test]
    async fn test_list_files_reuses_cached_file_list() {
        let ctx = SessionContext::new();
        let state = ctx.state();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        put_test_object(&store, "table/dt=2024-01-01/file1.parquet", b"a").await;

        let first_files = list_files(&state, &store, "memory:///table/dt=2024-01-01")
            .await
            .unwrap();
        assert_eq!(first_files.len(), 1);

        put_test_object(&store, "table/dt=2024-01-01/file2.parquet", b"b").await;

        let fresh_ctx = SessionContext::new();
        let fresh_state = fresh_ctx.state();
        let uncached_files = list_files(&fresh_state, &store, "memory:///table/dt=2024-01-01")
            .await
            .unwrap();
        assert_eq!(uncached_files.len(), 2);

        let cached_files = list_files(&state, &store, "memory:///table/dt=2024-01-01")
            .await
            .unwrap();
        assert_eq!(cached_files.len(), 1);
        assert_eq!(
            cached_files[0].location.as_ref(),
            "table/dt=2024-01-01/file1.parquet"
        );
    }

    #[tokio::test]
    async fn test_list_files_by_directories_across_multiple_dirs() {
        let ctx = SessionContext::new();
        let state = ctx.state();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        put_test_object(&store, "table/dt=2024-01-01/file1.parquet", b"a").await;
        put_test_object(&store, "table/dt=2024-01-02/file2.parquet", b"bb").await;

        let files = list_files_by_directories(
            &state,
            &store,
            vec![
                "memory:///table/dt=2024-01-01".to_string(),
                "memory:///table/dt=2024-01-02".to_string(),
            ],
        )
        .await
        .unwrap();

        assert_eq!(files.len(), 2);
        let mut paths: Vec<&str> = files.iter().map(|f| f.location.as_ref()).collect();
        paths.sort();
        assert_eq!(
            paths,
            vec![
                "table/dt=2024-01-01/file1.parquet",
                "table/dt=2024-01-02/file2.parquet",
            ]
        );
    }

    async fn put_test_object(store: &Arc<dyn ObjectStore>, path: &str, data: &[u8]) {
        store
            .put(&Path::from(path), data.to_vec().into())
            .await
            .unwrap();
    }
}
