use arrow_flight::sql::metadata::{SqlInfoData, SqlInfoDataBuilder};
use arrow_flight::sql::{SqlInfo, SqlSupportedTransaction};
use std::sync::LazyLock;

/// Server metadata served for `GetSqlInfo`. Lakelet's capabilities never
/// change at runtime, so the table is built once and shared by every request.
pub(super) static SQL_INFO_DATA: LazyLock<SqlInfoData> = LazyLock::new(|| {
    let mut builder = SqlInfoDataBuilder::new();
    builder.append(SqlInfo::FlightSqlServerName, "Lakelet");
    builder.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
    // The Arrow IPC format version, not the arrow crate version.
    builder.append(SqlInfo::FlightSqlServerArrowVersion, "1.3");
    builder.append(SqlInfo::FlightSqlServerReadOnly, true);
    builder.append(SqlInfo::FlightSqlServerSql, true);
    builder.append(
        SqlInfo::FlightSqlServerTransaction,
        SqlSupportedTransaction::None as i32,
    );
    builder.build().expect("static sql info must build")
});
