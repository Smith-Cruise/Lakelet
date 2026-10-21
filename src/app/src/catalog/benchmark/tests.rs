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
    sample_batches(table, opts, max_rows, (table.generate)(opts).unwrap())
}

fn sample_batches(
    table: &Table,
    opts: GenerationOptions,
    max_rows: usize,
    generator: BatchIterator,
) -> RecordBatch {
    let mut batches = Vec::new();
    let mut remaining = max_rows;
    for batch in generator {
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

// Keep the reference independent of Lakelet's range planner and of
// with_source_row_range, including the upstream adapter's own row count.
fn tpcds_unranged(table: &Table, opts: GenerationOptions) -> BatchIterator {
    let session = tpcdsgen::config::SessionBuilder::new()
        .with_scale_factor(opts.scale_factor)
        .build()
        .unwrap();
    macro_rules! adapter {
        ($adapter:ident) => {
            Box::new(
                tpcdsgen_arrow::$adapter::new(session)
                    .with_batch_size(opts.batch_size)
                    .map(|batch| batch.map_err(Into::into)),
            ) as BatchIterator
        };
    }
    match table.name {
        "call_center" => adapter!(CallCenterArrow),
        "catalog_page" => adapter!(CatalogPageArrow),
        "catalog_returns" => adapter!(CatalogReturnsArrow),
        "catalog_sales" => adapter!(CatalogSalesArrow),
        "customer" => adapter!(CustomerArrow),
        "customer_address" => adapter!(CustomerAddressArrow),
        "customer_demographics" => adapter!(CustomerDemographicsArrow),
        "date_dim" => adapter!(DateDimArrow),
        "household_demographics" => adapter!(HouseholdDemographicsArrow),
        "income_band" => adapter!(IncomeBandArrow),
        "inventory" => adapter!(InventoryArrow),
        "item" => adapter!(ItemArrow),
        "promotion" => adapter!(PromotionArrow),
        "reason" => adapter!(ReasonArrow),
        "ship_mode" => adapter!(ShipModeArrow),
        "store" => adapter!(StoreArrow),
        "store_returns" => adapter!(StoreReturnsArrow),
        "store_sales" => adapter!(StoreSalesArrow),
        "time_dim" => adapter!(TimeDimArrow),
        "warehouse" => adapter!(WarehouseArrow),
        "web_page" => adapter!(WebPageArrow),
        "web_returns" => adapter!(WebReturnsArrow),
        "web_sales" => adapter!(WebSalesArrow),
        "web_site" => adapter!(WebSiteArrow),
        _ => unreachable!(),
    }
}

#[test]
fn all_tables_generate_consistent_batches() {
    for table in tpch::TABLES.iter().chain(tpcds::TABLES) {
        let first = sample(table, options(17), 257);
        assert!(first.num_rows() > 0, "{}", table.name);
        assert_eq!(first, sample(table, options(64), 257), "{}", table.name);
    }

    // The v4 CLI creates fresh adapters for inclusive source-row ranges.
    // Check our scan starts against that path at fractional and larger scales,
    // and compare split windows with uninterrupted windows near the middle/end.
    // These are boundary samples, not full high-scale dataset comparisons.
    macro_rules! check_tpcds_ranges {
        ($adapter:ident, $source:ident, $parent:ident) => {{
            use tpcdsgen::config::{SessionBuilder, Table as SourceTable};
            let source = SourceTable::$source;
            let table = tpcds::TABLES
                .iter()
                .find(|t| t.name == source.get_name())
                .unwrap();
            for scale_factor in [0.01, 0.1, 1.5, 10.0, 100.0, 300.0, 1000.0] {
                let session = SessionBuilder::new()
                    .with_scale_factor(scale_factor)
                    .build()
                    .unwrap();
                let rows = session.get_scaling().get_row_count(source);
                assert_eq!(
                    rows,
                    session.get_scaling().get_row_count(SourceTable::$parent)
                );
                let opts = GenerationOptions {
                    scale_factor,
                    ..options(13)
                };
                let range = |start, end, max_rows| {
                    let batches = tpcdsgen_arrow::$adapter::new(session.clone())
                        .with_source_row_range(start, end)
                        .with_batch_size(opts.batch_size);
                    sample_batches(
                        table,
                        opts,
                        max_rows,
                        Box::new(batches.map(|batch| batch.map_err(Into::into))),
                    )
                };
                assert_eq!(
                    sample(table, opts, 257),
                    sample_batches(table, opts, 257, tpcds_unranged(table, opts)),
                    "{} SF{scale_factor} unranged prefix",
                    table.name
                );
                // Seven partitions exercise remainders and empty small-table partitions.
                for partition in [0, 3, 6] {
                    let start = 1 + rows / 7 * partition + partition.min(rows % 7);
                    let count = rows / 7 + u64::from(partition < rows % 7);
                    let expected = range(start, start + count - 1, 19);
                    let actual = sample(
                        table,
                        GenerationOptions {
                            partition: partition as i32,
                            partitions: 7,
                            ..opts
                        },
                        19,
                    );
                    assert_eq!(
                        actual, expected,
                        "{} SF{scale_factor} partition {partition}",
                        table.name
                    );
                }
                for end in [rows.min(17), rows / 2, rows] {
                    if end == 0 {
                        continue;
                    }
                    let start = end.saturating_sub(6).max(1);
                    let whole = range(start, end, usize::MAX);
                    let pieces: Vec<_> = (start..=end)
                        .map(|row| range(row, row, usize::MAX))
                        .collect();
                    assert_eq!(
                        whole,
                        concat_batches(&(table.schema)(), &pieces).unwrap(),
                        "{} SF{scale_factor} range {start}..={end}",
                        table.name
                    );
                }
            }
        }};
    }
    check_tpcds_ranges!(CallCenterArrow, CallCenter, CallCenter);
    check_tpcds_ranges!(CatalogPageArrow, CatalogPage, CatalogPage);
    check_tpcds_ranges!(CatalogReturnsArrow, CatalogReturns, CatalogSales);
    check_tpcds_ranges!(CatalogSalesArrow, CatalogSales, CatalogSales);
    check_tpcds_ranges!(CustomerArrow, Customer, Customer);
    check_tpcds_ranges!(CustomerAddressArrow, CustomerAddress, CustomerAddress);
    check_tpcds_ranges!(
        CustomerDemographicsArrow,
        CustomerDemographics,
        CustomerDemographics
    );
    check_tpcds_ranges!(DateDimArrow, DateDim, DateDim);
    check_tpcds_ranges!(
        HouseholdDemographicsArrow,
        HouseholdDemographics,
        HouseholdDemographics
    );
    check_tpcds_ranges!(IncomeBandArrow, IncomeBand, IncomeBand);
    check_tpcds_ranges!(InventoryArrow, Inventory, Inventory);
    check_tpcds_ranges!(ItemArrow, Item, Item);
    check_tpcds_ranges!(PromotionArrow, Promotion, Promotion);
    check_tpcds_ranges!(ReasonArrow, Reason, Reason);
    check_tpcds_ranges!(ShipModeArrow, ShipMode, ShipMode);
    check_tpcds_ranges!(StoreArrow, Store, Store);
    check_tpcds_ranges!(StoreReturnsArrow, StoreReturns, StoreSales);
    check_tpcds_ranges!(StoreSalesArrow, StoreSales, StoreSales);
    check_tpcds_ranges!(TimeDimArrow, TimeDim, TimeDim);
    check_tpcds_ranges!(WarehouseArrow, Warehouse, Warehouse);
    check_tpcds_ranges!(WebPageArrow, WebPage, WebPage);
    check_tpcds_ranges!(WebReturnsArrow, WebReturns, WebSales);
    check_tpcds_ranges!(WebSalesArrow, WebSales, WebSales);
    check_tpcds_ranges!(WebSiteArrow, WebSite, WebSite);
}

#[test]
fn benchmark_sf1_partition_equivalence() {
    // Hash complete rows with bounded memory, checking all partition boundaries
    // (including lineitem's variable rows per order and partsupp's four rows per part).
    for (kind, table) in [BenchmarkKind::Tpch, BenchmarkKind::Tpcds]
        .into_iter()
        .flat_map(|kind| kind.tables().iter().map(move |table| (kind, table)))
    {
        let digest = |partitions, scale_factor| {
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
                    scale_factor,
                    partition,
                    partitions,
                    ..options(8192)
                };
                let generator = if partitions == 1 && matches!(kind, BenchmarkKind::Tpcds) {
                    tpcds_unranged(table, opts)
                } else {
                    (table.generate)(opts).unwrap()
                };
                for batch in generator {
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
        let single = digest(1, 1.0);
        assert!(single.0 > 0, "{}", table.name);
        let expected = match (kind, table.name) {
            (BenchmarkKind::Tpch, "customer") => 150_000,
            (BenchmarkKind::Tpch, "lineitem") => 6_001_215,
            (BenchmarkKind::Tpch, "nation") => 25,
            (BenchmarkKind::Tpch, "orders") => 1_500_000,
            (BenchmarkKind::Tpch, "part") => 200_000,
            (BenchmarkKind::Tpch, "partsupp") => 800_000,
            (BenchmarkKind::Tpch, "region") => 5,
            (BenchmarkKind::Tpch, "supplier") => 10_000,
            (BenchmarkKind::Tpcds, _) => single.0,
            _ => unreachable!(),
        };
        assert_eq!(single.0, expected, "{}", table.name);
        let partitions = kind.partitions(table, 7).unwrap();
        assert_eq!(single, digest(partitions, 1.0), "{}", table.name);
        if matches!(kind, BenchmarkKind::Tpcds) {
            // Fractional scales are adapter-level coverage; catalog schemas
            // expose only the integer scales listed in SCALES.
            assert_eq!(
                digest(1, 0.01),
                digest(partitions, 0.01),
                "{} SF0.01",
                table.name
            );
        }
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
                let expected = if matches!(kind, BenchmarkKind::Tpch)
                    && matches!(table.name, "nation" | "region")
                {
                    1
                } else {
                    4
                };
                assert_eq!(scan.output_partitioning().partition_count(), expected);
                if *name == "sf1" {
                    let streams = (0..expected).map(|partition| {
                        let mut stream = scan.execute(partition, ctx.task_ctx()).unwrap();
                        async move { stream.next().await.unwrap().unwrap() }
                    });
                    let batches = futures::future::join_all(streams).await;
                    for (partition, batch) in batches.iter().enumerate() {
                        assert!(batch.num_rows() > 0, "{} partition {partition}", table.name);
                        assert_eq!(batch.schema(), provider.schema());
                        for previous in &batches[..partition] {
                            assert_ne!(batch, previous, "{} repeated partition", table.name);
                        }
                    }
                }
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
        let key = match name {
            "catalog_returns" => "cr_order_number, cr_item_sk",
            "store_returns" => "sr_ticket_number, sr_item_sk",
            "web_returns" => "wr_order_number, wr_item_sk",
            "inventory" => "inv_date_sk, inv_item_sk, inv_warehouse_sk",
            "item" => "i_item_sk",
            _ => unreachable!(),
        };
        let sql = format!("SELECT * FROM tpcds.sf1.{name} ORDER BY {key} LIMIT 33");
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
    // Require all workers to enter generation before any can emit a batch.
    // This detects serialization even when the plan advertises multiple partitions.
    static ENTERED: AtomicUsize = AtomicUsize::new(0);
    for kind in [BenchmarkKind::Tpch, BenchmarkKind::Tpcds] {
        ENTERED.store(0, Ordering::SeqCst);
        let provider = BenchmarkTableProvider {
            kind,
            table: Table {
                generate: |opts| {
                    ENTERED.fetch_add(1, Ordering::SeqCst);
                    let start = std::time::Instant::now();
                    while ENTERED.load(Ordering::SeqCst) < 4 {
                        if start.elapsed() > Duration::from_secs(10) {
                            return exec_err!("generation partitions did not run concurrently");
                        }
                        std::thread::yield_now();
                    }
                    let batch = RecordBatch::try_new(
                        synthetic_schema(),
                        vec![Arc::new(Int64Array::from(vec![opts.partition as i64]))],
                    )?;
                    Ok(Box::new(std::iter::once(Ok(batch))))
                },
                ..table
            },
            schema: synthetic_schema(),
            scale_factor: 1.0,
            cpu_handle: Handle::current(),
        };
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
        let plan = provider.scan(&ctx.state(), None, &[], None).await.unwrap();
        let streams = (0..4).map(|partition| {
            let mut stream = plan.execute(partition, ctx.task_ctx()).unwrap();
            async move { stream.next().await.unwrap().unwrap() }
        });
        for (partition, batch) in futures::future::join_all(streams).await.iter().enumerate() {
            assert_eq!(
                array_value_to_string(batch.column(0), 0).unwrap(),
                partition.to_string()
            );
        }
        assert_eq!(ENTERED.load(Ordering::SeqCst), 4);
    }
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
