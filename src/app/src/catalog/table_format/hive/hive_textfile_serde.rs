use datafusion::common::{DataFusionError, Result};
use std::collections::HashMap;

/// LazySimpleSerDe properties of a Hive TextFile table, resolved from the
/// merged table and SerDe parameters. A property that is not set takes
/// Hive's default.
#[derive(Debug, Clone, PartialEq)]
pub struct TextFileSerdeProperties {
    /// `field.delim`, falling back to `serialization.format`; `\x01` by default.
    pub field_delimiter: u8,
    /// `line.delim`; `\n` by default.
    pub line_delimiter: u8,
    /// `escape.delim`; Hive has no escape character unless one is set.
    pub escape_delimiter: Option<u8>,
    /// `serialization.null.format`; `\N` by default.
    pub null_format: String,
    /// `skip.header.line.count`; 0 by default.
    pub skip_header_line_count: u64,
}

impl Default for TextFileSerdeProperties {
    fn default() -> Self {
        Self {
            field_delimiter: b'\x01',
            line_delimiter: b'\n',
            escape_delimiter: None,
            null_format: "\\N".to_string(),
            skip_header_line_count: 0,
        }
    }
}

impl TextFileSerdeProperties {
    /// Fails only when a value cannot be parsed, e.g. a non-numeric
    /// `skip.header.line.count`.
    pub fn try_new(properties: &HashMap<String, String>) -> Result<Self> {
        let defaults = Self::default();
        let delimiter = |keys: &[&str]| {
            keys.iter()
                .find_map(|key| properties.get(*key))
                .and_then(|raw| parse_hive_delimiter(raw))
        };

        let skip_header_line_count = match properties.get("skip.header.line.count") {
            Some(count) => count.trim().parse::<u64>().map_err(|_| {
                DataFusionError::Plan(format!(
                    "invalid Hive TextFile skip.header.line.count: {count:?}"
                ))
            })?,
            None => defaults.skip_header_line_count,
        };

        Ok(Self {
            field_delimiter: delimiter(&[
                "field.delim",
                "serialization.format",
                "serdeConstants.FIELD_DELIM",
                "columns.delimited.by",
            ])
            .unwrap_or(defaults.field_delimiter),
            line_delimiter: delimiter(&["line.delim"]).unwrap_or(defaults.line_delimiter),
            escape_delimiter: delimiter(&["escape.delim"]),
            null_format: properties
                .get("serialization.null.format")
                .cloned()
                .unwrap_or(defaults.null_format),
            skip_header_line_count,
        })
    }
}

/// Parses a delimiter the way Hive's `LazySerDeParameters.getByte` does: a
/// decimal byte value first, otherwise the first character. Escaped forms such
/// as `\t`, `\001`, `\u0001` and `0x01` are accepted for hand-written metadata.
fn parse_hive_delimiter(raw: &str) -> Option<u8> {
    if raw.is_empty() {
        return None;
    }

    if let Ok(value) = raw.parse::<i8>() {
        return Some(value as u8);
    }

    if raw.len() > 1 {
        if let Some(hex) = raw.strip_prefix("\\u") {
            return u8::from_str_radix(hex, 16).ok();
        }
        if let Some(escaped) = raw.strip_prefix('\\') {
            if escaped.chars().all(|c| ('0'..='7').contains(&c)) {
                return u8::from_str_radix(escaped, 8).ok();
            }
            return match escaped {
                "t" => Some(b'\t'),
                "n" => Some(b'\n'),
                "r" => Some(b'\r'),
                _ => None,
            };
        }
        if let Some(hex) = raw.strip_prefix("0x") {
            return u8::from_str_radix(hex, 16).ok();
        }
    }

    // Hive keeps the low byte of the first UTF-16 code unit.
    raw.chars().next().map(|c| c as u32 as u8)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn serde(pairs: &[(&str, &str)]) -> Result<TextFileSerdeProperties> {
        TextFileSerdeProperties::try_new(
            &pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    #[test]
    fn test_parse_hive_delimiter() {
        // Hive's getByte parses a decimal byte value before taking the first char.
        assert_eq!(parse_hive_delimiter("1"), Some(0x01));
        assert_eq!(parse_hive_delimiter("9"), Some(b'\t'));
        assert_eq!(parse_hive_delimiter("124"), Some(b'|'));
        assert_eq!(parse_hive_delimiter("|"), Some(b'|'));
        assert_eq!(parse_hive_delimiter(","), Some(b','));
        // Whitespace delimiters must be kept as-is.
        assert_eq!(parse_hive_delimiter("\t"), Some(b'\t'));
        assert_eq!(parse_hive_delimiter(" "), Some(b' '));
        assert_eq!(parse_hive_delimiter("\u{1}"), Some(0x01));
        // Escaped forms from hand-written metadata.
        assert_eq!(parse_hive_delimiter("\\t"), Some(b'\t'));
        assert_eq!(parse_hive_delimiter("\\001"), Some(0x01));
        assert_eq!(parse_hive_delimiter("\\u0001"), Some(0x01));
        assert_eq!(parse_hive_delimiter("0x7C"), Some(b'|'));
        assert_eq!(parse_hive_delimiter(""), None);
    }

    #[test]
    fn test_resolve_field_delimiter() {
        assert_eq!(serde(&[]).unwrap(), TextFileSerdeProperties::default());
        // Hive's default TextFile tables only carry serialization.format=1.
        assert_eq!(
            serde(&[("serialization.format", "1")])
                .unwrap()
                .field_delimiter,
            0x01
        );
        assert_eq!(
            serde(&[("field.delim", "|"), ("serialization.format", ",")])
                .unwrap()
                .field_delimiter,
            b'|'
        );
    }

    #[test]
    fn test_resolve_other_properties() {
        let properties = serde(&[
            ("line.delim", "\r"),
            ("escape.delim", "\\"),
            ("serialization.null.format", ""),
            ("skip.header.line.count", "2"),
        ])
        .unwrap();
        assert_eq!(
            properties,
            TextFileSerdeProperties {
                field_delimiter: 0x01,
                line_delimiter: b'\r',
                escape_delimiter: Some(b'\\'),
                null_format: String::new(),
                skip_header_line_count: 2,
            }
        );

        let err = serde(&[("skip.header.line.count", "abc")]).unwrap_err();
        assert!(err.to_string().contains("skip.header.line.count"), "{err}");
    }
}
