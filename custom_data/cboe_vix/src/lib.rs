//! CBOE VIX daily OHLC custom data source plugin for rlean.
//!
//! Fetches the free VIX history CSV published by CBOE at:
//! `https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv`
//!
//! The file contains the full history going back to January 1990 and is
//! updated daily by CBOE. No API key is required.
//!
//! CSV format (dates in MM/DD/YYYY):
//! ```text
//! DATE,OPEN,HIGH,LOW,CLOSE
//! 01/02/1990,17.24,17.24,17.24,17.24
//! 01/03/1990,18.19,18.34,17.44,17.91
//! ```
//!
//! # Usage
//! ```python
//! vix = self.add_data("cboe_vix", "VIX")
//! # In on_data: data.custom["VIX"].value  (== CLOSE)
//! #             data.custom["VIX"].fields["high"]
//! ```

use chrono::NaiveDate;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;

use lean_data::custom::{CustomDataConfig, CustomDataFormat, CustomDataPoint, CustomDataSource, CustomDataTransport};
use lean_data_providers::ICustomDataSource;
use lean_plugin::{PluginKind, rlean_plugin};

// ---------------------------------------------------------------------------
// CBOE VIX implementation
// ---------------------------------------------------------------------------

pub const CBOE_VIX_URL: &str =
    "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv";

/// CBOE VIX custom data source.
///
/// The `ticker` parameter in `get_source` is ignored because there is only
/// one series. The same URL is always returned.
pub struct CboeVixDataSource;

impl CboeVixDataSource {
    pub fn new() -> Self { CboeVixDataSource }
}

impl Default for CboeVixDataSource {
    fn default() -> Self { Self::new() }
}

impl ICustomDataSource for CboeVixDataSource {
    fn name(&self) -> &str { "cboe_vix" }

    fn get_source(
        &self,
        _ticker: &str,
        _date: NaiveDate,
        _config: &CustomDataConfig,
    ) -> Option<CustomDataSource> {
        Some(CustomDataSource {
            uri: CBOE_VIX_URL.to_string(),
            transport: CustomDataTransport::Http,
            format: CustomDataFormat::Csv,
        })
    }

    /// Parse one CSV line from the CBOE VIX history file.
    ///
    /// Expected format: `MM/DD/YYYY,OPEN,HIGH,LOW,CLOSE`
    /// `value` is set to CLOSE. All four OHLC values are available in `fields`.
    fn reader(
        &self,
        line: &str,
        date: NaiveDate,
        _config: &CustomDataConfig,
    ) -> Option<CustomDataPoint> {
        let line = line.trim();
        if line.is_empty() || line.starts_with("DATE") {
            return None;
        }

        let mut parts = line.splitn(5, ',');
        let date_str  = parts.next()?.trim();
        let open_str  = parts.next()?.trim();
        let high_str  = parts.next()?.trim();
        let low_str   = parts.next()?.trim();
        let close_str = parts.next()?.trim();

        let parsed_date = NaiveDate::parse_from_str(date_str, "%m/%d/%Y").ok()?;
        if parsed_date != date {
            return None;
        }

        let open  = Decimal::from_str(open_str).ok()?;
        let high  = Decimal::from_str(high_str).ok()?;
        let low   = Decimal::from_str(low_str).ok()?;
        let close = Decimal::from_str(close_str).ok()?;

        let mut fields = HashMap::new();
        fields.insert("open".to_string(),  serde_json::json!(open.to_string()));
        fields.insert("high".to_string(),  serde_json::json!(high.to_string()));
        fields.insert("low".to_string(),   serde_json::json!(low.to_string()));
        fields.insert("close".to_string(), serde_json::json!(close.to_string()));

        Some(CustomDataPoint { time: parsed_date, value: close, fields })
    }

    fn is_full_history_source(&self) -> bool { true }

    /// Parse one CSV line from the full CBOE history without filtering by date.
    ///
    /// Expected format: `MM/DD/YYYY,OPEN,HIGH,LOW,CLOSE`. Sets `time` from the line.
    fn read_history_line(
        &self,
        line: &str,
        config: &CustomDataConfig,
    ) -> Option<CustomDataPoint> {
        // Reuse reader() with the date parsed from the line itself.
        let line = line.trim();
        if line.is_empty() || line.starts_with("DATE") {
            return None;
        }
        let date_str = line.splitn(2, ',').next()?.trim();
        let date = NaiveDate::parse_from_str(date_str, "%m/%d/%Y").ok()?;
        self.reader(line, date, config)
    }
}

// ---------------------------------------------------------------------------
// Plugin ABI exports
// ---------------------------------------------------------------------------

rlean_plugin! {
    name    = "cboe_vix",
    version = "0.1.0",
    kind    = PluginKind::CustomData,
}

/// C-stable factory: allocate a `CboeVixDataSource` and return a thin pointer.
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
    let source: Box<dyn ICustomDataSource> = Box::new(CboeVixDataSource::new());
    Box::into_raw(Box::new(source)) as *mut ()
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn make_config() -> CustomDataConfig {
        CustomDataConfig {
            ticker: "VIX".to_string(),
            source_type: "cboe_vix".to_string(),
            resolution: lean_core::Resolution::Daily,
            properties: HashMap::new(),
        }
    }

    fn date(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    #[test]
    fn get_source_returns_cboe_url() {
        let result = CboeVixDataSource::new().get_source("VIX", date(2024, 1, 2), &make_config()).unwrap();
        assert_eq!(result.uri, CBOE_VIX_URL);
    }

    #[test]
    fn get_source_same_url_regardless_of_ticker_or_date() {
        let src = CboeVixDataSource::new();
        let cfg = make_config();
        let url1 = src.get_source("VIX",     date(1990, 1, 2),  &cfg).unwrap().uri;
        let url2 = src.get_source("anything", date(2023, 12, 29), &cfg).unwrap().uri;
        assert_eq!(url1, url2);
        assert_eq!(url1, CBOE_VIX_URL);
    }

    #[test]
    fn reader_parses_valid_line() {
        let src = CboeVixDataSource::new();
        let target = date(1990, 1, 2);
        let point = src.reader("01/02/1990,17.24,17.24,17.24,17.24", target, &make_config()).unwrap();
        assert_eq!(point.time, target);
        assert_eq!(point.value, Decimal::from_str("17.24").unwrap());
        assert_eq!(point.fields["close"], serde_json::json!("17.24"));
    }

    #[test]
    fn reader_parses_different_ohlc_values() {
        let src = CboeVixDataSource::new();
        let target = date(2020, 3, 16);
        let point = src.reader("03/16/2020,82.69,85.47,74.53,82.69", target, &make_config()).unwrap();
        assert_eq!(point.value, Decimal::from_str("82.69").unwrap());
        assert_eq!(point.fields["high"], serde_json::json!("85.47"));
        assert_eq!(point.fields["low"],  serde_json::json!("74.53"));
    }

    #[test]
    fn reader_skips_header_line() {
        assert!(CboeVixDataSource::new().reader("DATE,OPEN,HIGH,LOW,CLOSE", date(2024, 1, 2), &make_config()).is_none());
    }

    #[test]
    fn reader_returns_none_for_wrong_date() {
        assert!(CboeVixDataSource::new().reader("01/02/1990,17.24,17.24,17.24,17.24", date(1990, 1, 3), &make_config()).is_none());
    }

    #[test]
    fn reader_returns_none_for_empty_line() {
        assert!(CboeVixDataSource::new().reader("", date(2024, 1, 2), &make_config()).is_none());
    }

    #[test]
    fn reader_returns_none_for_malformed_line() {
        assert!(CboeVixDataSource::new().reader("not-a-date,x,y,z,w", date(2024, 1, 2), &make_config()).is_none());
    }

    #[test]
    fn reader_returns_none_for_incomplete_columns() {
        // Missing CLOSE column.
        assert!(CboeVixDataSource::new().reader("01/02/1990,17.24,17.24,17.24", date(1990, 1, 2), &make_config()).is_none());
    }

    #[test]
    fn plugin_name() {
        assert_eq!(CboeVixDataSource::new().name(), "cboe_vix");
    }
}
