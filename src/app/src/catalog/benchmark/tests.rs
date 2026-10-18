use super::*;
use crate::catalog::{CatalogConfigs, CatalogManager, LakeletCatalogProviderList};
use crate::context::LakeletContext;
use crate::sql::session::ExtendedSessionContext;
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::exec_err;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::StreamExt;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

fn options(batch_size: usize) -> GenerationOptions {
    GenerationOptions {
        scale_factor: 1.0,
        partition: 0,
        partitions: 1,
        batch_size,
    }
}

fn sample(table: &Table, opts: GenerationOptions, max_rows: usize) -> RecordBatch {
    let mut batches = Vec::new();
    let mut remaining = max_rows;
    for batch in (table.generate)(opts).unwrap() {
        let batch = batch.unwrap();
        assert!(
            batch.num_rows() > 0 && batch.num_rows() <= opts.batch_size,
            "{}",
            table.name
        );
        assert_eq!(batch.schema(), (table.schema)(), "{}", table.name);
        let count = remaining.min(batch.num_rows());
        batches.push(batch.slice(0, count));
        remaining -= count;
        if remaining == 0 {
            break;
        }
    }
    concat_batches(&(table.schema)(), &batches).unwrap()
}

#[test]
fn all_tables_generate_consistent_batches() {
    for table in tpch::TABLES.iter().chain(tpcds::TABLES) {
        let first = sample(table, options(17), 257);
        assert!(first.num_rows() > 0, "{}", table.name);
        assert_eq!(first, sample(table, options(64), 257), "{}", table.name);
    }
}

#[test]
fn tpch_sf1_partition_equivalence() {
    // Hash complete rows with bounded memory, checking all partition boundaries
    // (including lineitem's variable rows per order and partsupp's four rows per part).
    for table in tpch::TABLES {
        let digest = |partitions| {
            let converter = RowConverter::new(
                (table.schema)()
                    .fields()
                    .iter()
                    .map(|f| SortField::new(f.data_type().clone()))
                    .collect(),
            )
            .unwrap();
            let mut count = 0;
            let mut sum = 0_u64;
            for partition in 0..partitions {
                let opts = GenerationOptions {
                    partition,
                    partitions,
                    ..options(8192)
                };
                for batch in (table.generate)(opts).unwrap() {
                    let batch = batch.unwrap();
                    count += batch.num_rows();
                    for row in converter.convert_columns(batch.columns()).unwrap().iter() {
                        let mut hash = DefaultHasher::new();
                        row.as_ref().hash(&mut hash);
                        sum = sum.wrapping_add(hash.finish());
                    }
                }
            }
            (count, sum)
        };
        let single = digest(1);
        let expected = match table.name {
            "customer" => 150_000,
            "lineitem" => 6_001_215,
            "nation" => 25,
            "orders" => 1_500_000,
            "part" => 200_000,
            "partsupp" => 800_000,
            "region" => 5,
            "supplier" => 10_000,
            _ => unreachable!(),
        };
        assert_eq!(single.0, expected, "{}", table.name);
        let partitions = BenchmarkKind::Tpch.partitions(table, 3).unwrap();
        assert_eq!(single, digest(partitions), "{}", table.name);
    }
}

#[tokio::test]
async fn catalog_metadata_and_partition_policy() {
    for (kind, count) in [(BenchmarkKind::Tpch, 8), (BenchmarkKind::Tpcds, 24)] {
        let catalog = BenchmarkCatalog::new(kind, Handle::current());
        assert_eq!(
            catalog.list_schema_names().await.unwrap(),
            ["sf1", "sf10", "sf100", "sf300", "sf1000"]
        );
        for name in ["tiny", "sf0", "sf2", "sf01", "SF1"] {
            assert!(!catalog.schema_exist(name).await.unwrap());
            assert!(catalog.schema(name).await.unwrap().is_none());
            assert!(catalog.list_table_names(name).await.is_err());
        }
        for (name, _) in SCALES {
            assert_eq!(catalog.list_table_names(name).await.unwrap().len(), count);
            let schema = catalog.schema(name).await.unwrap().unwrap();
            assert!(schema.table("dbgen_version").await.unwrap().is_none());
            let ctx =
                SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
            for table in kind.tables() {
                let provider = schema.table(table.name).await.unwrap().unwrap();
                assert_eq!(provider.schema(), (table.schema)());
                assert!(provider.get_table_definition().is_none());
                let scan = provider.scan(&ctx.state(), None, &[], None).await.unwrap();
                let expected = if matches!(kind, BenchmarkKind::Tpcds)
                    || matches!(table.name, "nation" | "region")
                {
                    1
                } else {
                    4
                };
                assert_eq!(scan.output_partitioning().partition_count(), expected);
            }
        }
    }
}

fn sql_context() -> ExtendedSessionContext {
    let configs: CatalogConfigs =
        toml::from_str("[[tpch]]\nname = 'tpch'\n[[tpcds]]\nname = 'tpcds'").unwrap();
    let mut manager = CatalogManager::new();
    manager.load_catalogs(&configs).unwrap();
    let context = Arc::new(LakeletContext {
        catalog_manager: Arc::new(manager),
        ..Default::default()
    });
    let catalogs = Arc::new(LakeletCatalogProviderList::new(context.clone()).unwrap());
    ExtendedSessionContext::new(catalogs, context, Arc::new(RuntimeEnv::default()))
}

async fn query(ctx: &ExtendedSessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql).await.unwrap().collect().await.unwrap()
}

#[tokio::test]
async fn sql_discovery_projection_filters_limits_and_read_only() {
    let ctx = sql_context();
    for sql in [
        "SHOW CATALOGS",
        "SHOW SCHEMAS FROM tpch",
        "SHOW TABLES FROM tpcds.sf1",
        "DESCRIBE tpch.sf1.lineitem",
        "EXPLAIN SELECT * FROM tpcds.sf1000.store_sales",
    ] {
        assert!(!query(&ctx, sql).await.is_empty(), "{sql}");
    }
    for sql in [
        "SELECT * FROM tpch.sf1000.lineitem LIMIT 0",
        "SELECT * FROM tpcds.sf1000.store_sales LIMIT 0",
    ] {
        assert_eq!(
            query(&ctx, sql)
                .await
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            0
        );
    }
    let rows = query(
        &ctx,
        "SELECT n_name FROM tpch.sf1.nation WHERE n_nationkey >= 20 LIMIT 3",
    )
    .await;
    assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    assert_eq!(rows[0].num_columns(), 1);
    let count = query(&ctx, "SELECT COUNT(*) FROM tpch.sf1.nation").await;
    assert_eq!(array_value_to_string(count[0].column(0), 0).unwrap(), "25");
    let join = query(&ctx, "SELECT COUNT(*) FROM tpch.sf1.nation n JOIN tpch.sf1.region r ON n.n_regionkey = r.r_regionkey").await;
    assert_eq!(array_value_to_string(join[0].column(0), 0).unwrap(), "25");
    for name in [
        "catalog_returns",
        "store_returns",
        "web_returns",
        "inventory",
        "item",
    ] {
        let sql = format!("SELECT * FROM tpcds.sf1.{name} LIMIT 33");
        query(&ctx, "SET datafusion.execution.target_partitions = 1").await;
        let a = query(&ctx, &sql).await;
        query(&ctx, "SET datafusion.execution.target_partitions = 4").await;
        let b = query(&ctx, &sql).await;
        assert_eq!(
            concat_batches(&a[0].schema(), &a).unwrap(),
            concat_batches(&b[0].schema(), &b).unwrap(),
            "{name}"
        );
    }
    for sql in [
        "CREATE SCHEMA tpch.new_schema",
        "DROP TABLE tpch.sf1.nation",
        "CREATE TABLE tpch.sf1.new_table (id BIGINT)",
        "INSERT INTO tpch.sf1.region VALUES (5, 'NEW', 'test')",
        "SHOW CREATE TABLE tpch.sf1.nation",
        "SELECT * FROM tpch.tiny.nation",
    ] {
        let result = match ctx.sql(sql).await {
            Ok(df) => df.collect().await.map(|_| ()),
            Err(e) => Err(e),
        };
        assert!(result.is_err(), "{sql}");
    }
}

fn synthetic_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

#[tokio::test]
async fn lazy_generation_errors_and_cancellation() {
    static INITIALIZED: AtomicUsize = AtomicUsize::new(0);
    static GENERATED: AtomicUsize = AtomicUsize::new(0);
    static DROPPED: AtomicBool = AtomicBool::new(false);
    struct Infinite;
    impl Iterator for Infinite {
        type Item = Result<RecordBatch>;
        fn next(&mut self) -> Option<Self::Item> {
            GENERATED.fetch_add(1, Ordering::SeqCst);
            Some(
                RecordBatch::try_new(
                    synthetic_schema(),
                    vec![Arc::new(Int64Array::from(vec![1]))],
                )
                .map_err(Into::into),
            )
        }
    }
    impl Drop for Infinite {
        fn drop(&mut self) {
            DROPPED.store(true, Ordering::SeqCst);
        }
    }
    let table = Table {
        name: "synthetic",
        schema: synthetic_schema,
        generate: |_| {
            INITIALIZED.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(Infinite))
        },
    };
    let provider = BenchmarkTableProvider {
        kind: BenchmarkKind::Tpcds,
        table,
        schema: synthetic_schema(),
        scale_factor: 1.0,
        cpu_handle: Handle::current(),
    };
    let ctx = SessionContext::new();
    let plan = provider.scan(&ctx.state(), None, &[], None).await.unwrap();
    let zero = provider
        .scan(&ctx.state(), Some(&vec![]), &[], Some(0))
        .await
        .unwrap();
    assert!(
        zero.execute(0, ctx.task_ctx())
            .unwrap()
            .next()
            .await
            .is_none()
    );
    assert_eq!(GENERATED.load(Ordering::SeqCst), 0);
    assert_eq!(INITIALIZED.load(Ordering::SeqCst), 0);
    let mut stream = plan.execute(0, ctx.task_ctx()).unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap().num_rows(), 1);
    drop(stream);
    tokio::time::timeout(Duration::from_secs(10), async {
        while !DROPPED.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(GENERATED.load(Ordering::SeqCst) <= 3);
    assert_eq!(INITIALIZED.load(Ordering::SeqCst), 1);
    for generate in [
        (|_| exec_err!("initialization failed")) as fn(GenerationOptions) -> Result<BatchIterator>,
        |_| Ok(Box::new(std::iter::once(exec_err!("generation failed"))) as BatchIterator),
        |_| panic!("initialization failed"),
        |_| {
            Ok(
                Box::new(std::iter::from_fn(|| -> Option<Result<RecordBatch>> {
                    panic!("generation failed")
                })) as BatchIterator,
            )
        },
    ] {
        let partition = BenchmarkPartitionStream {
            table: Table { generate, ..table },
            schema: synthetic_schema(),
            options: options(1),
            cpu_handle: Handle::current(),
        };
        let mut stream = partition.execute(ctx.task_ctx());
        assert!(
            stream
                .next()
                .await
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("failed")
        );
        assert!(stream.next().await.is_none());
    }
}
