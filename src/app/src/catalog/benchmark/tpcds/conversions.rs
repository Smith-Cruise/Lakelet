// Adapted from tpcdsgen-arrow 0.1.0-alpha.1 (Apache-2.0).
// Source: https://crates.io/crates/tpcdsgen-arrow/0.1.0-alpha.1
// Local changes: Arrow 58, Utf8 strings, static schemas and fallible batches.
// See LICENSE in this directory for the upstream license.

//! Routines to convert TPC-DS types to Arrow types

use datafusion::arrow::array::{Int32Array, StringArray, StringBuilder};
use tpcdsgen::types::{Address, Date, Decimal};

/// Julian day number for the Unix epoch (1970-01-01)
const UNIX_EPOCH_JULIAN: i32 = 2440588;

/// Convert a TPC-DS Decimal to an i128 value suitable for Decimal128Array.
///
/// TPC-DS Decimal stores the number as an unscaled integer (e.g. 1234 means 12.34
/// when precision=2). Arrow Decimal128 also stores unscaled integers, so we just
/// cast the i64 to i128.
#[inline(always)]
pub fn decimal_to_i128(d: Decimal) -> i128 {
    d.get_number() as i128
}

/// Convert a TPC-DS Date to an Arrow Date32 (days since Unix epoch 1970-01-01).
///
/// TPC-DS Date is stored as a Julian day number internally. Julian day 2440588
/// corresponds to 1970-01-01.
#[inline(always)]
pub fn date_to_date32(d: &Date) -> i32 {
    d.to_julian_days() - UNIX_EPOCH_JULIAN
}

/// Convert a Julian day i64 to an Arrow Date32. Returns None if julian_days < 0.
#[inline(always)]
pub fn julian_to_date32(julian_days: i64) -> Option<i32> {
    if julian_days < 0 {
        None
    } else {
        Some(julian_days as i32 - UNIX_EPOCH_JULIAN)
    }
}

/// Build a StringArray from an iterator of optional &str values (nullable).
pub fn string_array_from_opt_iter<'a, I>(values: I) -> StringArray
where
    I: Iterator<Item = Option<&'a str>>,
{
    let values: Vec<Option<&str>> = values.collect();
    let size_hint = values.len();
    let mut builder = StringBuilder::with_capacity(size_hint, 0);
    for v in values {
        match v {
            Some(s) => builder.append_value(s),
            None => builder.append_null(),
        }
    }
    builder.finish()
}

/// Convert a boolean value to "Y" or "N" static str.
#[inline(always)]
pub fn bool_to_yn(b: bool) -> &'static str {
    if b { "Y" } else { "N" }
}

/// Check whether the bit at `pos` is set in a null bitmap, indicating a NULL value.
#[inline(always)]
pub fn is_null(nbm: i64, pos: u32) -> bool {
    (nbm >> pos) & 1 != 0
}

/// Return `Some(val)` unless the null bitmap bit at `pos` is set.
#[inline(always)]
pub fn opt<T>(nbm: i64, pos: u32, val: T) -> Option<T> {
    if is_null(nbm, pos) { None } else { Some(val) }
}

/// Return `Some(sk)` unless null bitmap bit is set OR sk < 0 (sentinel for absent FK).
#[inline(always)]
pub fn sk_opt(nbm: i64, pos: u32, sk: i64) -> Option<i64> {
    if is_null(nbm, pos) || sk < 0 {
        None
    } else {
        Some(sk)
    }
}

/// Expand an [`Address`] into 10 individual column arrays (street_number, street_name,
/// street_type, suite_number, city, county, state, zip, country, gmt_offset).
///
/// Returns `(Int32Array, [StringArray; 8], Int32Array)`.
pub fn address_columns<'a>(
    rows: impl Iterator<Item = (&'a Address, i64, u32)> + 'a,
) -> (
    Int32Array,
    StringArray,
    StringArray,
    StringArray,
    StringArray,
    StringArray,
    StringArray,
    StringArray,
    StringArray,
    Int32Array,
) {
    let rows: Vec<_> = rows.collect();
    let street_number = Int32Array::from_iter(rows.iter().map(|(a, nbm, base)| {
        if is_null(*nbm, *base) {
            None
        } else {
            Some(a.get_street_number())
        }
    }));
    let mut street_name_b = StringBuilder::with_capacity(rows.len(), 0);
    let mut street_type_b = StringBuilder::with_capacity(rows.len(), 0);
    let mut suite_number_b = StringBuilder::with_capacity(rows.len(), 0);
    let mut city_b = StringBuilder::with_capacity(rows.len(), 0);
    let mut county_b = StringBuilder::with_capacity(rows.len(), 0);
    let mut state_b = StringBuilder::with_capacity(rows.len(), 0);
    let mut zip_b = StringBuilder::with_capacity(rows.len(), 0);
    let mut country_b = StringBuilder::with_capacity(rows.len(), 0);

    // Each address sub-field has its own null bit at base+offset (0=street_number,
    // 1=street_name, ..., 9=gmt_offset), matching the per-column null_bit_map layout.
    for (a, nbm, base) in &rows {
        if is_null(*nbm, *base + 1) {
            street_name_b.append_null();
        } else {
            street_name_b.append_value(a.get_street_name());
        }
        if is_null(*nbm, *base + 2) {
            street_type_b.append_null();
        } else {
            street_type_b.append_value(a.get_street_type());
        }
        if is_null(*nbm, *base + 3) {
            suite_number_b.append_null();
        } else {
            suite_number_b.append_value(a.get_suite_number());
        }
        if is_null(*nbm, *base + 4) {
            city_b.append_null();
        } else {
            city_b.append_value(a.get_city());
        }
        match a.get_county() {
            Some(c) if !is_null(*nbm, *base + 5) => county_b.append_value(c),
            _ => county_b.append_null(),
        }
        if is_null(*nbm, *base + 6) {
            state_b.append_null();
        } else {
            state_b.append_value(a.get_state());
        }
        if is_null(*nbm, *base + 7) {
            zip_b.append_null();
        } else {
            zip_b.append_value(format!("{:05}", a.get_zip()));
        }
        if is_null(*nbm, *base + 8) {
            country_b.append_null();
        } else {
            country_b.append_value(a.get_country());
        }
    }
    let gmt_offset = Int32Array::from_iter(rows.iter().map(|(a, nbm, base)| {
        if is_null(*nbm, *base + 9) {
            None
        } else {
            Some(a.get_gmt_offset())
        }
    }));
    (
        street_number,
        street_name_b.finish(),
        street_type_b.finish(),
        suite_number_b.finish(),
        city_b.finish(),
        county_b.finish(),
        state_b.finish(),
        zip_b.finish(),
        country_b.finish(),
        gmt_offset,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decimal_to_i128() {
        let d = Decimal::new(12345, 2).unwrap();
        assert_eq!(decimal_to_i128(d), 12345);
        assert_eq!(decimal_to_i128(Decimal::new(-12345, 2).unwrap()), -12345);
    }

    #[test]
    fn test_julian_to_date32_epoch() {
        // Julian day 2440588 = 1970-01-01, so offset should be 0
        assert_eq!(julian_to_date32(2440588), Some(0));
        assert_eq!(julian_to_date32(2440587), Some(-1));
    }

    #[test]
    fn test_julian_to_date32_negative() {
        assert_eq!(julian_to_date32(-1), None);
    }

    #[test]
    fn test_bool_to_yn() {
        assert_eq!(bool_to_yn(true), "Y");
        assert_eq!(bool_to_yn(false), "N");
    }

    #[test]
    fn test_null_bitmap_and_missing_foreign_keys() {
        assert_eq!(opt(1 << 5, 5, 42), None);
        assert_eq!(opt(1 << 5, 4, 42), Some(42));
        assert_eq!(sk_opt(0, 0, -1), None);
        assert_eq!(sk_opt(1, 0, 42), None);
        assert_eq!(sk_opt(0, 0, 0), Some(0));
        assert_eq!(sk_opt(0, 0, 42), Some(42));
    }
}
