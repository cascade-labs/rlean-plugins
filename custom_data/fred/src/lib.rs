//! FRED (Federal Reserve Economic Data) custom data source plugin for rlean.
//!
//! Provides free public macroeconomic time series from the St. Louis Fed —
//! e.g., UNRATE (unemployment), CPIAUCSL (CPI), DGS10 (10-yr treasury), FEDFUNDS.
//!
//! The FRED `fredgraph.csv` endpoint returns the *entire* history for a series
//! in a single CSV download.  No API key is required for this public endpoint.
//!
//! # Usage
//! ```python
//! fred = self.add_data("fred", "UNRATE")
//! # In on_data: data.custom["UNRATE"].value
//! ```

use chrono::NaiveDate;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;

use lean_data::custom::{
    CustomDataConfig, CustomDataFormat, CustomDataPoint, CustomDataSource, CustomDataTransport,
};
use lean_data_providers::ICustomDataSource;
use lean_plugin::{rlean_plugin, PluginKind};

// ---------------------------------------------------------------------------
// FRED implementation
// ---------------------------------------------------------------------------

/// FRED custom data source.
///
/// `ticker` is treated as the FRED series ID (e.g. "UNRATE", "CPIAUCSL").
/// The full-history CSV is fetched once; `reader()` filters to the requested date.
pub struct FredDataSource;

impl FredDataSource {
    pub fn new() -> Self {
        FredDataSource
    }

    /// Build the FRED CSV download URL for `series_id`.
    ///
    /// Uses the public `fredgraph.csv` endpoint which does not require an API key.
    /// If `api_key` is present in properties, the official FRED API endpoint is
    /// used instead, which offers additional filtering options.
    pub fn build_url(series_id: &str, api_key: Option<&str>) -> String {
        match api_key {
            Some(key) => format!(
                "https://api.stlouisfed.org/fred/series/observations\
                ?series_id={series_id}&api_key={key}&file_type=json"
            ),
            None => format!("https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"),
        }
    }
}

impl Default for FredDataSource {
    fn default() -> Self {
        Self::new()
    }
}

impl ICustomDataSource for FredDataSource {
    fn name(&self) -> &str {
        "fred"
    }

    /// Return the FRED full-history CSV URL.
    ///
    /// FRED serves the entire series in one file, so the same URL is returned
    /// regardless of `date`. The runner caches results per-date in Parquet.
    fn get_source(
        &self,
        ticker: &str,
        _date: NaiveDate,
        config: &CustomDataConfig,
    ) -> Option<CustomDataSource> {
        let api_key = config.properties.get("api_key").map(|s| s.as_str());
        let uri = Self::build_url(ticker, api_key);
        Some(CustomDataSource {
            uri,
            transport: CustomDataTransport::Http,
            format: CustomDataFormat::Csv,
        })
    }

    /// Parse one CSV line from the FRED `fredgraph.csv` response.
    ///
    /// Expected format: `DATE,VALUE` where DATE is `YYYY-MM-DD`.
    ///
    /// Returns `None` for:
    /// - The header line (starts with "DATE")
    /// - Lines whose date does not match the requested `date`
    /// - Malformed lines
    /// - Missing / "." value placeholders (FRED uses "." for missing observations)
    fn reader(
        &self,
        line: &str,
        date: NaiveDate,
        config: &CustomDataConfig,
    ) -> Option<CustomDataPoint> {
        let line = line.trim();
        if line.is_empty() || line.starts_with("DATE") {
            return None;
        }

        let mut parts = line.splitn(2, ',');
        let date_str = parts.next()?.trim();
        let value_str = parts.next()?.trim();

        let parsed_date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d").ok()?;
        if parsed_date != date {
            return None;
        }

        // FRED uses "." to indicate a missing observation.
        if value_str == "." {
            return None;
        }

        let value = Decimal::from_str(value_str).ok()?;

        let mut fields = HashMap::new();
        fields.insert(
            "series_id".to_string(),
            serde_json::Value::String(config.ticker.clone()),
        );

        Some(CustomDataPoint {
            time: parsed_date,
            end_time: None,
            value,
            fields,
        })
    }

    fn is_full_history_source(&self) -> bool {
        true
    }

    /// Parse one CSV line from the full FRED history without filtering by date.
    ///
    /// Expected format: `YYYY-MM-DD,VALUE`. Sets `time` from the date in the line.
    fn read_history_line(&self, line: &str, config: &CustomDataConfig) -> Option<CustomDataPoint> {
        let line = line.trim();
        if line.is_empty() || line.starts_with("DATE") {
            return None;
        }

        let mut parts = line.splitn(2, ',');
        let date_str = parts.next()?.trim();
        let value_str = parts.next()?.trim();

        let date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d").ok()?;

        if value_str == "." {
            return None;
        }

        let value = Decimal::from_str(value_str).ok()?;

        let mut fields = HashMap::new();
        fields.insert(
            "series_id".to_string(),
            serde_json::Value::String(config.ticker.clone()),
        );

        Some(CustomDataPoint {
            time: date,
            end_time: None,
            value,
            fields,
        })
    }
}

// ---------------------------------------------------------------------------
// Plugin ABI exports
// ---------------------------------------------------------------------------

rlean_plugin! {
    name    = "fred",
    version = "0.1.0",
    kind    = PluginKind::CustomData,
}

/// C-stable factory: allocate a `FredDataSource` and return a thin pointer.
///
/// Returns `*mut ()` pointing to a heap-allocated `Box<dyn ICustomDataSource>`.
/// Double-boxed so that only a thin (8-byte) pointer crosses the FFI boundary —
/// fat pointers (`*mut dyn Trait`) are not C-ABI-safe.
///
/// # Safety
/// The returned pointer must be freed by the loader via:
/// `*Box::from_raw(raw as *mut Box<dyn ICustomDataSource>)`
#[no_mangle]
pub extern "C" fn rlean_custom_data_factory() -> *mut () {
    let source: Box<dyn ICustomDataSource> = Box::new(FredDataSource::new());
    Box::into_raw(Box::new(source)) as *mut ()
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn make_config(ticker: &str) -> CustomDataConfig {
        CustomDataConfig {
            ticker: ticker.to_string(),
            source_type: "fred".to_string(),
            resolution: lean_core::Resolution::Daily,
            properties: HashMap::new(),
        }
    }

    fn make_config_with_api_key(ticker: &str, key: &str) -> CustomDataConfig {
        let mut props = HashMap::new();
        props.insert("api_key".to_string(), key.to_string());
        CustomDataConfig {
            ticker: ticker.to_string(),
            source_type: "fred".to_string(),
            resolution: lean_core::Resolution::Daily,
            properties: props,
        }
    }

    fn date(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    #[test]
    fn get_source_returns_fredgraph_url_without_api_key() {
        let src = FredDataSource::new();
        let config = make_config("UNRATE");
        let result = src.get_source("UNRATE", date(2024, 1, 1), &config).unwrap();
        assert_eq!(
            result.uri,
            "https://fred.stlouisfed.org/graph/fredgraph.csv?id=UNRATE"
        );
    }

    #[test]
    fn get_source_returns_api_url_with_api_key() {
        let src = FredDataSource::new();
        let config = make_config_with_api_key("DGS10", "mykey123");
        let result = src.get_source("DGS10", date(2024, 6, 1), &config).unwrap();
        assert!(result.uri.contains("api.stlouisfed.org"));
        assert!(result.uri.contains("DGS10"));
        assert!(result.uri.contains("mykey123"));
    }

    #[test]
    fn get_source_same_url_for_different_dates() {
        let src = FredDataSource::new();
        let config = make_config("FEDFUNDS");
        let url1 = src
            .get_source("FEDFUNDS", date(2020, 1, 1), &config)
            .unwrap()
            .uri;
        let url2 = src
            .get_source("FEDFUNDS", date(2023, 6, 15), &config)
            .unwrap()
            .uri;
        assert_eq!(url1, url2);
    }

    #[test]
    fn reader_parses_valid_line() {
        let src = FredDataSource::new();
        let config = make_config("UNRATE");
        let target = date(2024, 1, 1);
        let point = src
            .reader("2024-01-01,3.7", target, &config)
            .expect("should parse");
        assert_eq!(point.time, target);
        assert_eq!(point.value, Decimal::from_str("3.7").unwrap());
        assert_eq!(
            point.fields["series_id"],
            serde_json::Value::String("UNRATE".to_string())
        );
    }

    #[test]
    fn reader_skips_header_line() {
        let src = FredDataSource::new();
        let config = make_config("UNRATE");
        assert!(src
            .reader("DATE,UNRATE", date(2024, 1, 1), &config)
            .is_none());
    }

    #[test]
    fn reader_returns_none_for_wrong_date() {
        let src = FredDataSource::new();
        let config = make_config("CPIAUCSL");
        assert!(src
            .reader("2024-01-01,310.326", date(2024, 2, 1), &config)
            .is_none());
    }

    #[test]
    fn reader_returns_none_for_missing_observation() {
        let src = FredDataSource::new();
        let config = make_config("DGS10");
        assert!(src
            .reader("2024-03-15,.", date(2024, 3, 15), &config)
            .is_none());
    }

    #[test]
    fn reader_returns_none_for_empty_line() {
        let src = FredDataSource::new();
        let config = make_config("UNRATE");
        assert!(src.reader("", date(2024, 1, 1), &config).is_none());
    }

    #[test]
    fn reader_returns_none_for_malformed_line() {
        let src = FredDataSource::new();
        let config = make_config("UNRATE");
        assert!(src
            .reader("not-a-date,not-a-number", date(2024, 1, 1), &config)
            .is_none());
    }

    #[test]
    fn reader_parses_large_value() {
        let src = FredDataSource::new();
        let config = make_config("CPIAUCSL");
        let target = date(2023, 12, 1);
        let point = src
            .reader("2023-12-01,310.326", target, &config)
            .expect("should parse");
        assert_eq!(point.value, Decimal::from_str("310.326").unwrap());
    }

    #[test]
    fn plugin_name() {
        assert_eq!(FredDataSource::new().name(), "fred");
    }
}
