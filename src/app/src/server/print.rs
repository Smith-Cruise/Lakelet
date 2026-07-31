use comfy_table::modifiers::{UTF8_ROUND_CORNERS, UTF8_SOLID_INNER_BORDERS};
use comfy_table::presets::UTF8_FULL;
use comfy_table::{Cell, ContentArrangement, Table};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions as ArrowFormatOptions};
use datafusion::common::Result;
use datafusion::config::FormatOptions;
use datafusion_cli::object_storage::instrumented::{InstrumentedObjectStoreMode, RequestSummaries};
use datafusion_cli::print_options::{MaxRows, PrintOptions};
use std::io::{self, Write};
use std::time::Instant;

const OBJECT_STORE_PROFILING_HEADER: &str = "Object Store Profiling";

/// Print result batches followed by the execution footer (and object store
/// profiling when enabled). Replaces `PrintOptions::print_batches` for the
/// interactive REPL so wide tables can adapt to the terminal width.
pub fn print_results(
    print_options: &PrintOptions,
    schema: SchemaRef,
    batches: &[RecordBatch],
    row_count: usize,
    query_start_time: Instant,
    format_options: &FormatOptions,
) -> Result<()> {
    let stdout = io::stdout();
    let mut writer = stdout.lock();

    let options: ArrowFormatOptions = format_options.try_into()?;
    let batches: Vec<RecordBatch> = batches
        .iter()
        .filter(|b| b.num_rows() > 0)
        .cloned()
        .collect();

    // An empty result set still prints the column headers, unless the schema
    // itself is empty.
    if !batches.is_empty() || !schema.fields().is_empty() {
        let table = format_table(&schema, &batches, &options, None)?;
        writeln!(writer, "{table}")?;
    }

    let details = execution_details(row_count, print_options.maxrows, query_start_time);
    write_footer(&mut writer, print_options, &details)
}

/// Build a comfy-table for the batches: solid box-drawing borders with
/// rounded corners and a line between rows, so wrapped multi-line rows stay
/// distinguishable. With `width` unset, the table adapts to the terminal
/// width at render time (comfy-table's `tty` feature); tests pass an explicit
/// `width` for deterministic assertions.
fn format_table(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    options: &ArrowFormatOptions,
    width: Option<u16>,
) -> Result<Table> {
    let mut table = Table::new();
    table.load_preset(UTF8_FULL);
    table.apply_modifier(UTF8_SOLID_INNER_BORDERS);
    table.apply_modifier(UTF8_ROUND_CORNERS);
    table.set_content_arrangement(ContentArrangement::Dynamic);
    if let Some(width) = width {
        table.set_width(width);
    }

    let mut header = Vec::new();
    for field in schema.fields() {
        if options.types_info() {
            header.push(Cell::new(format!(
                "{}\n{}",
                field.name(),
                field.data_type()
            )));
        } else {
            header.push(Cell::new(field.name()));
        }
    }
    table.set_header(header);

    for batch in batches {
        let formatters = batch
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), options))
            .collect::<Result<Vec<_>, _>>()?;
        for row in 0..batch.num_rows() {
            let mut cells = Vec::new();
            for formatter in &formatters {
                cells.push(Cell::new(formatter.value(row)));
            }
            table.add_row(cells);
        }
    }

    Ok(table)
}

// Byte-identical replica of datafusion-cli's `get_execution_details_formatted`
// (private in that crate), so the footer keeps its exact current shape.
fn execution_details(row_count: usize, maxrows: MaxRows, query_start_time: Instant) -> String {
    let nrows_shown_msg = match maxrows {
        MaxRows::Limited(nrows) if nrows < row_count => {
            format!("(First {nrows} displayed. Use --maxrows to adjust)")
        }
        _ => String::new(),
    };

    format!(
        "{} row(s) fetched. {}\nElapsed {:.3} seconds.\n",
        row_count,
        nrows_shown_msg,
        query_start_time.elapsed().as_secs_f64()
    )
}

// Byte-identical replica of datafusion-cli's `PrintOptions::write_output`.
fn write_footer<W: Write>(
    writer: &mut W,
    print_options: &PrintOptions,
    formatted_exec_details: &str,
) -> Result<()> {
    if !print_options.quiet {
        writeln!(writer, "{formatted_exec_details}")?;

        let instrument_mode = print_options.instrumented_registry.instrument_mode();
        if instrument_mode != InstrumentedObjectStoreMode::Disabled {
            writeln!(writer, "{OBJECT_STORE_PROFILING_HEADER}")?;
            for store in print_options.instrumented_registry.stores() {
                let requests = store.take_requests();

                if !requests.is_empty() {
                    writeln!(writer, "{store}")?;
                    if instrument_mode == InstrumentedObjectStoreMode::Trace {
                        for req in requests.iter() {
                            writeln!(writer, "{req}")?;
                        }
                        writeln!(writer)?;
                    }

                    writeln!(writer, "Summaries:")?;
                    let summaries = RequestSummaries::new(&requests);
                    writeln!(writer, "{summaries}")?;
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("description", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![
                    Some("Shared others could not count fully dollars. New members ca"),
                    None,
                ])),
            ],
        )
        .expect("created record batch")
    }

    #[test]
    fn table_wraps_within_width() {
        let batch = sample_batch();
        let options = ArrowFormatOptions::default();
        let table = format_table(
            &batch.schema(),
            std::slice::from_ref(&batch),
            &options,
            Some(40),
        )
        .expect("formatted table");
        let rendered = table.to_string();

        for line in rendered.lines() {
            assert!(
                line.chars().count() <= 40,
                "line exceeds width 40: {line:?}"
            );
        }
        // The long value must survive wrapping: strip layout characters and
        // check the concatenated content is intact.
        let flattened: String = rendered
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || *c == '.')
            .collect();
        let expected: String = "Shared others could not count fully dollars. New members ca"
            .chars()
            .filter(|c| *c != ' ')
            .collect();
        assert!(
            flattened.contains(&expected),
            "wrapped table lost cell content: {rendered}"
        );
    }

    #[test]
    fn table_uses_solid_borders_with_row_separators() {
        let batch = sample_batch();
        let options = ArrowFormatOptions::default();
        let mut table = format_table(
            &batch.schema(),
            std::slice::from_ref(&batch),
            &options,
            None,
        )
        .expect("formatted table");
        table.force_no_tty();
        let rendered = table.to_string();

        assert!(
            rendered.starts_with('╭'),
            "missing round corner: {rendered}"
        );
        assert!(rendered.contains('╞'), "missing header line: {rendered}");
        // 2 data rows -> exactly one solid separator line between them.
        let separator_lines = rendered
            .lines()
            .filter(|line| line.starts_with('├'))
            .count();
        assert_eq!(separator_lines, 1, "unexpected layout: {rendered}");
    }

    #[test]
    fn table_header_only_when_empty() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let options = ArrowFormatOptions::default();
        let mut table = format_table(&schema, &[], &options, None).expect("formatted table");
        table.force_no_tty();
        let rendered = table.to_string();

        assert!(rendered.contains("id"), "missing header: {rendered}");
        // Top border, header, header separator, bottom border - no data rows.
        assert_eq!(rendered.lines().count(), 4, "unexpected layout: {rendered}");
    }

    #[test]
    fn null_rendering_follows_format_options() {
        let batch = sample_batch();
        let options = ArrowFormatOptions::default().with_null("NULL");

        let mut table =
            format_table(&batch.schema(), &[batch], &options, None).expect("formatted table");
        table.force_no_tty();
        assert!(table.to_string().contains("NULL"));
    }

    #[test]
    fn execution_details_format() {
        let details = execution_details(3, MaxRows::Unlimited, Instant::now());
        assert!(details.starts_with("3 row(s) fetched. \nElapsed "));
        assert!(details.ends_with(" seconds.\n"));

        let limited = execution_details(10, MaxRows::Limited(5), Instant::now());
        assert!(limited.starts_with(
            "10 row(s) fetched. (First 5 displayed. Use --maxrows to adjust)\nElapsed "
        ));
    }
}
