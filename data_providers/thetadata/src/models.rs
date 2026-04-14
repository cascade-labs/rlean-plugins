/// ThetaData API response models.
///
/// The v3 API returns NDJSON — one JSON object per line — so all wire types
/// derive `Deserialize` for line-by-line parsing.
use chrono::{NaiveDate, NaiveDateTime};
use serde::{Deserialize, Serialize};

// ─── V3 wire types ────────────────────────────────────────────────────────────

/// One row from `option/history/quote`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V3OptionQuote {
    #[serde(default)]
    pub symbol: String,
    #[serde(default)]
    pub expiration: String,
    #[serde(default)]
    pub strike: f64,
    #[serde(default)]
    pub right: String,
    #[serde(default)]
    pub bid_size: f64,
    #[serde(default)]
    pub bid_exchange: u8,
    #[serde(rename = "bid", default)]
    pub bid_price: f64,
    #[serde(default)]
    pub bid_condition: i32,
    #[serde(default)]
    pub ask_size: f64,
    #[serde(default)]
    pub ask_exchange: u8,
    #[serde(rename = "ask", default)]
    pub ask_price: f64,
    #[serde(default)]
    pub ask_condition: i32,
    #[serde(default)]
    pub date: String,
    #[serde(default)]
    pub ms_of_day: u32,
    #[serde(default)]
    pub timestamp: String,
}

/// One row from `option/history/trade`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V3OptionTrade {
    #[serde(default)]
    pub symbol: String,
    #[serde(default)]
    pub expiration: String,
    #[serde(default)]
    pub strike: f64,
    #[serde(default)]
    pub right: String,
    #[serde(default)]
    pub size: f64,
    #[serde(default)]
    pub exchange: u8,
    #[serde(default)]
    pub condition: i32,
    #[serde(default)]
    pub price: f64,
    #[serde(default)]
    pub sequence: i64,
    #[serde(default)]
    pub date: String,
    #[serde(default)]
    pub ms_of_day: u32,
    #[serde(default)]
    pub timestamp: String,
}

/// One row from `option/history/ohlc`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V3OptionOhlc {
    #[serde(default)]
    pub symbol: String,
    #[serde(default)]
    pub expiration: String,
    #[serde(default)]
    pub strike: f64,
    #[serde(default)]
    pub right: String,
    #[serde(default)]
    pub timestamp: String,
    #[serde(default)]
    pub date: String,
    #[serde(default)]
    pub ms_of_day: u32,
    #[serde(default)]
    pub open: f64,
    #[serde(default)]
    pub high: f64,
    #[serde(default)]
    pub low: f64,
    #[serde(default)]
    pub close: f64,
    #[serde(default)]
    pub volume: f64,
    #[serde(default)]
    pub count: u32,
}

/// One row from `option/history/eod`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V3OptionEod {
    #[serde(default)]
    pub symbol: String,
    #[serde(default)]
    pub expiration: String,
    #[serde(default)]
    pub strike: f64,
    #[serde(default)]
    pub right: String,
    #[serde(default)]
    pub date: String,
    #[serde(default)]
    pub open: f64,
    #[serde(default)]
    pub high: f64,
    #[serde(default)]
    pub low: f64,
    #[serde(default)]
    pub close: f64,
    #[serde(default)]
    pub volume: f64,
    #[serde(default)]
    pub count: u32,
    #[serde(default)]
    pub bid_size: f64,
    #[serde(default)]
    pub bid_exchange: u8,
    #[serde(rename = "bid", default)]
    pub bid_price: f64,
    #[serde(default)]
    pub bid_condition: i32,
    #[serde(default)]
    pub ask_size: f64,
    #[serde(default)]
    pub ask_exchange: u8,
    #[serde(rename = "ask", default)]
    pub ask_price: f64,
    #[serde(default)]
    pub ask_condition: i32,
    #[serde(default)]
    pub created: String,
    #[serde(default)]
    pub last_trade: String,
}

/// One row from `index/history/price`.
#[derive(Debug, Clone, Deserialize)]
pub struct V3IndexPrice {
    #[serde(default)]
    pub timestamp: String,
    #[serde(default)]
    pub price: f64,
}

// ─── Normalized output types ──────────────────────────────────────────────────

/// NBBO quote snapshot — v2 shape, converted from v3 wire types.
#[derive(Debug, Clone)]
pub struct QuoteBar {
    pub date: NaiveDate,
    /// Milliseconds since midnight ET.
    pub ms_of_day: u32,
    pub bid_size: f64,
    pub bid_exchange: u8,
    pub bid_price: f64,
    pub bid_condition: i32,
    pub ask_size: f64,
    pub ask_exchange: u8,
    pub ask_price: f64,
    pub ask_condition: i32,
}

impl QuoteBar {
    pub fn datetime_et(&self) -> Option<NaiveDateTime> {
        self.date.and_hms_milli_opt(0, 0, 0, 0).map(|t| {
            t + chrono::Duration::milliseconds(self.ms_of_day as i64)
        })
    }
}

/// OHLCV bar.
#[derive(Debug, Clone)]
pub struct OhlcBar {
    pub date: NaiveDate,
    pub ms_of_day: u32,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub count: u32,
}

/// Trade tick.
#[derive(Debug, Clone)]
pub struct TradeTick {
    pub date: NaiveDate,
    pub ms_of_day: u32,
    pub price: f64,
    pub size: f64,
    pub exchange: u8,
    pub condition: i32,
}

/// End-of-day snapshot (stock or option).
#[derive(Debug, Clone)]
pub struct EodBar {
    pub date: NaiveDate,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub count: u32,
    pub bid_price: f64,
    pub bid_size: f64,
    pub ask_price: f64,
    pub ask_size: f64,
}

/// Single index price point.
#[derive(Debug, Clone)]
pub struct IndexPrice {
    pub timestamp: NaiveDateTime,
    pub price: f64,
}

/// Open interest point.
#[derive(Debug, Clone)]
pub struct OpenInterest {
    pub timestamp: NaiveDateTime,
    pub value: f64,
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

/// Parse a `yyyyMMdd` date string or fall back to an ISO timestamp.
pub fn parse_date(date: &str, timestamp: &str) -> Option<NaiveDate> {
    let clean = date.replace('-', "");
    if clean.len() == 8 {
        if let Ok(d) = NaiveDate::parse_from_str(&clean, "%Y%m%d") {
            return Some(d);
        }
    }
    // Fall back: parse the ISO timestamp and take the date part.
    if !timestamp.is_empty() {
        if let Ok(dt) = NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%dT%H:%M:%S%.f") {
            return Some(dt.date());
        }
        if let Ok(dt) = NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%d %H:%M:%S%.f") {
            return Some(dt.date());
        }
    }
    None
}

/// Parse milliseconds-of-day from an ISO timestamp, or return 0.
pub fn ms_of_day_from_timestamp(timestamp: &str) -> u32 {
    if timestamp.is_empty() {
        return 0;
    }
    for fmt in &["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"] {
        if let Ok(dt) = NaiveDateTime::parse_from_str(timestamp, fmt) {
            use chrono::Timelike;
            return dt.time().num_seconds_from_midnight() * 1000
                + dt.time().nanosecond() / 1_000_000;
        }
    }
    0
}

/// Normalize a right string (`"C"`, `"call"` → `"c"`; `"P"`, `"put"` → `"p"`).
pub fn normalize_right(right: &str) -> &'static str {
    match right.trim().to_lowercase().as_str() {
        "c" | "call" => "c",
        "p" | "put"  => "p",
        _            => "c",
    }
}

/// Strike in milli-dollars → dollars (ThetaData stores strikes × 1000).
pub fn normalize_strike(strike_millidollars: f64) -> f64 {
    strike_millidollars / 1000.0
}

/// Normalize an expiration string (remove hyphens, trim whitespace).
pub fn normalize_expiration(exp: &str) -> String {
    exp.replace('-', "").trim().to_string()
}

/// Exchange code → exchange name.
pub fn exchange_name(code: u8) -> &'static str {
    match code {
        1  => "NQEX", 2  => "NQAD", 3  => "NYSE",  4  => "AMEX",
        5  => "CBOE", 6  => "ISEX", 7  => "PACF",  8  => "CINC",
        9  => "PHIL", 10 => "OPRA", 11 => "BOST",  12 => "NQNM",
        13 => "NQSC", 14 => "NQBB", 15 => "NQPK",  16 => "NQIX",
        17 => "CHIC", 18 => "TSE",  19 => "CDNX",  20 => "CME",
        21 => "NYBT", 22 => "MRCY", 23 => "COMX",  24 => "CBOT",
        25 => "NYMX", 26 => "KCBT", 27 => "MGEX",  28 => "NYBO",
        42 => "C2",   43 => "MIAX", 54 => "CFE",   60 => "BATS",
        63 => "BATY", 64 => "EDGE", 65 => "EDGX",  68 => "IEX",
        73 => "MEMX", 75 => "LTSE",
        _  => "",
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    // ── parse_date ─────────────────────────────────────────────────────────────

    #[test]
    fn test_parse_date_yyyymmdd() {
        let d = parse_date("20240115", "").expect("should parse yyyyMMdd");
        assert_eq!(d, NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
    }

    #[test]
    fn test_parse_date_with_hyphens() {
        // Some v3 rows return "2024-01-15" — hyphens are stripped before parsing.
        let d = parse_date("2024-01-15", "").expect("should parse yyyy-MM-dd");
        assert_eq!(d, NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
    }

    #[test]
    fn test_parse_date_falls_back_to_iso_timestamp() {
        let d = parse_date("", "2024-03-22T14:30:00.000").expect("should fall back to timestamp");
        assert_eq!(d, NaiveDate::from_ymd_opt(2024, 3, 22).unwrap());
    }

    #[test]
    fn test_parse_date_space_separated_timestamp() {
        let d = parse_date("", "2024-06-07 09:45:00.000").expect("space-separated timestamp");
        assert_eq!(d, NaiveDate::from_ymd_opt(2024, 6, 7).unwrap());
    }

    #[test]
    fn test_parse_date_empty_inputs_returns_none() {
        assert!(parse_date("", "").is_none());
    }

    #[test]
    fn test_parse_date_invalid_returns_none() {
        assert!(parse_date("not-a-date", "also-bad").is_none());
    }

    // ── ms_of_day_from_timestamp ───────────────────────────────────────────────

    #[test]
    fn test_ms_of_day_market_open() {
        // 09:30:00.000 ET = 9*3600 + 30*60 = 34_200 seconds → 34_200_000 ms
        let ms = ms_of_day_from_timestamp("2024-01-15T09:30:00.000");
        assert_eq!(ms, 34_200_000);
    }

    #[test]
    fn test_ms_of_day_with_sub_second_precision() {
        // 14:30:00.250 → (14*3600 + 30*60)*1000 + 250 = 52_200_250 ms
        let ms = ms_of_day_from_timestamp("2024-01-15T14:30:00.250");
        assert_eq!(ms, 52_200_250);
    }

    #[test]
    fn test_ms_of_day_midnight() {
        let ms = ms_of_day_from_timestamp("2024-01-15T00:00:00.000");
        assert_eq!(ms, 0);
    }

    #[test]
    fn test_ms_of_day_empty_string_returns_zero() {
        assert_eq!(ms_of_day_from_timestamp(""), 0);
    }

    #[test]
    fn test_ms_of_day_invalid_returns_zero() {
        assert_eq!(ms_of_day_from_timestamp("garbage"), 0);
    }

    // ── normalize_right ────────────────────────────────────────────────────────

    #[test]
    fn test_normalize_right_uppercase_c() {
        assert_eq!(normalize_right("C"), "c");
    }

    #[test]
    fn test_normalize_right_uppercase_p() {
        assert_eq!(normalize_right("P"), "p");
    }

    #[test]
    fn test_normalize_right_call_long() {
        assert_eq!(normalize_right("call"), "c");
    }

    #[test]
    fn test_normalize_right_put_long() {
        assert_eq!(normalize_right("put"), "p");
    }

    #[test]
    fn test_normalize_right_call_mixed_case() {
        assert_eq!(normalize_right("Call"), "c");
    }

    #[test]
    fn test_normalize_right_unknown_defaults_to_call() {
        assert_eq!(normalize_right("X"), "c");
        assert_eq!(normalize_right(""), "c");
    }

    // ── normalize_strike ───────────────────────────────────────────────────────

    #[test]
    fn test_normalize_strike_round() {
        // ThetaData stores strikes as milli-dollars.  450000 → $450.00
        assert!((normalize_strike(450_000.0) - 450.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_normalize_strike_fractional() {
        // $127.50 stored as 127500
        assert!((normalize_strike(127_500.0) - 127.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_normalize_strike_zero() {
        assert!((normalize_strike(0.0)).abs() < f64::EPSILON);
    }

    // ── normalize_expiration ───────────────────────────────────────────────────

    #[test]
    fn test_normalize_expiration_removes_hyphens() {
        assert_eq!(normalize_expiration("2024-01-19"), "20240119");
    }

    #[test]
    fn test_normalize_expiration_already_clean() {
        assert_eq!(normalize_expiration("20240119"), "20240119");
    }

    #[test]
    fn test_normalize_expiration_trims_whitespace() {
        assert_eq!(normalize_expiration("  20240119  "), "20240119");
    }

    // ── exchange_name ──────────────────────────────────────────────────────────

    #[test]
    fn test_exchange_name_known_codes() {
        assert_eq!(exchange_name(3),  "NYSE");
        assert_eq!(exchange_name(5),  "CBOE");
        assert_eq!(exchange_name(60), "BATS");
        assert_eq!(exchange_name(43), "MIAX");
    }

    #[test]
    fn test_exchange_name_unknown_code_empty() {
        assert_eq!(exchange_name(255), "");
        assert_eq!(exchange_name(0),   "");
    }

    // ── V3 wire type deserialization ───────────────────────────────────────────

    #[test]
    fn test_deserialize_v3_option_ohlc_complete_row() {
        let json = r#"{
            "symbol": "AAPL",
            "expiration": "20240119",
            "strike": 185000.0,
            "right": "C",
            "timestamp": "2024-01-15T10:00:00.000",
            "date": "20240115",
            "ms_of_day": 36000000,
            "open": 2.50,
            "high": 3.10,
            "low": 2.40,
            "close": 2.90,
            "volume": 1500.0,
            "count": 42
        }"#;

        let row: V3OptionOhlc = serde_json::from_str(json).expect("should deserialize");
        assert_eq!(row.symbol, "AAPL");
        assert_eq!(row.expiration, "20240119");
        assert!((row.strike - 185_000.0).abs() < f64::EPSILON);
        assert_eq!(row.right, "C");
        assert!((row.open   - 2.50).abs() < f64::EPSILON);
        assert!((row.high   - 3.10).abs() < f64::EPSILON);
        assert!((row.low    - 2.40).abs() < f64::EPSILON);
        assert!((row.close  - 2.90).abs() < f64::EPSILON);
        assert!((row.volume - 1500.0).abs() < f64::EPSILON);
        assert_eq!(row.count, 42);
        assert_eq!(row.ms_of_day, 36_000_000);
    }

    #[test]
    fn test_deserialize_v3_option_ohlc_missing_optional_fields() {
        // Only mandatory business fields present; serde `default` fills the rest.
        let json = r#"{"open": 1.0, "high": 1.5, "low": 0.9, "close": 1.2}"#;
        let row: V3OptionOhlc = serde_json::from_str(json).expect("should deserialize with defaults");
        assert_eq!(row.symbol, "");
        assert_eq!(row.ms_of_day, 0);
        assert_eq!(row.count, 0);
    }

    #[test]
    fn test_deserialize_v3_option_quote_bid_ask_aliases() {
        // The wire type uses `#[serde(rename = "bid")]` for bid_price.
        let json = r#"{
            "symbol": "SPY",
            "expiration": "20240119",
            "strike": 460000.0,
            "right": "P",
            "bid": 1.25,
            "ask": 1.30,
            "bid_size": 10.0,
            "ask_size": 20.0,
            "bid_exchange": 5,
            "ask_exchange": 5,
            "bid_condition": 0,
            "ask_condition": 0,
            "date": "20240115",
            "ms_of_day": 0,
            "timestamp": "2024-01-15T10:00:00.000"
        }"#;

        let row: V3OptionQuote = serde_json::from_str(json).expect("should deserialize bid/ask aliases");
        assert!((row.bid_price - 1.25).abs() < f64::EPSILON);
        assert!((row.ask_price - 1.30).abs() < f64::EPSILON);
        assert_eq!(row.right, "P");
    }

    #[test]
    fn test_deserialize_v3_option_trade() {
        let json = r#"{
            "symbol": "TSLA",
            "expiration": "20240202",
            "strike": 200000.0,
            "right": "C",
            "size": 5.0,
            "exchange": 60,
            "condition": 0,
            "price": 12.75,
            "sequence": 123456789,
            "date": "20240115",
            "ms_of_day": 45000000,
            "timestamp": "2024-01-15T12:30:00.000"
        }"#;

        let row: V3OptionTrade = serde_json::from_str(json).expect("should deserialize trade row");
        assert_eq!(row.symbol, "TSLA");
        assert!((row.price - 12.75).abs() < f64::EPSILON);
        assert!((row.size  - 5.0).abs()   < f64::EPSILON);
        assert_eq!(row.exchange, 60);
        assert_eq!(row.sequence, 123_456_789);
    }

    #[test]
    fn test_deserialize_v3_option_eod_with_bid_ask() {
        let json = r#"{
            "symbol": "NVDA",
            "expiration": "20240315",
            "strike": 650000.0,
            "right": "C",
            "date": "20240115",
            "open": 45.0,
            "high": 50.0,
            "low": 44.0,
            "close": 48.0,
            "volume": 800.0,
            "count": 20,
            "bid_size": 5.0,
            "bid_exchange": 5,
            "bid": 47.80,
            "bid_condition": 0,
            "ask_size": 5.0,
            "ask_exchange": 5,
            "ask": 48.20,
            "ask_condition": 0,
            "created": "2024-01-16T08:00:00.000",
            "last_trade": "2024-01-15T15:59:00.000"
        }"#;

        let row: V3OptionEod = serde_json::from_str(json).expect("should deserialize eod row");
        assert!((row.bid_price - 47.80).abs() < f64::EPSILON);
        assert!((row.ask_price - 48.20).abs() < f64::EPSILON);
        assert_eq!(row.count, 20);
    }

    #[test]
    fn test_deserialize_v3_index_price() {
        let json = r#"{"timestamp": "2024-01-15T10:00:00.000", "price": 4750.25}"#;
        let row: V3IndexPrice = serde_json::from_str(json).expect("should deserialize index price");
        assert!((row.price - 4750.25).abs() < f64::EPSILON);
        assert_eq!(row.timestamp, "2024-01-15T10:00:00.000");
    }

    // ── QuoteBar::datetime_et ──────────────────────────────────────────────────

    #[test]
    fn test_quote_bar_datetime_et_at_market_open() {
        // 09:30 ET = 34_200_000 ms
        let bar = QuoteBar {
            date: NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            ms_of_day: 34_200_000,
            bid_size: 10.0, bid_exchange: 3, bid_price: 99.50, bid_condition: 0,
            ask_size: 10.0, ask_exchange: 3, ask_price: 99.60, ask_condition: 0,
        };
        let dt = bar.datetime_et().expect("should produce datetime");
        assert_eq!(dt.date(), NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
        use chrono::Timelike;
        assert_eq!(dt.hour(), 9);
        assert_eq!(dt.minute(), 30);
        assert_eq!(dt.second(), 0);
    }

    #[test]
    fn test_quote_bar_datetime_et_midnight() {
        let bar = QuoteBar {
            date: NaiveDate::from_ymd_opt(2024, 6, 1).unwrap(),
            ms_of_day: 0,
            bid_size: 1.0, bid_exchange: 0, bid_price: 1.0, bid_condition: 0,
            ask_size: 1.0, ask_exchange: 0, ask_price: 1.0, ask_condition: 0,
        };
        let dt = bar.datetime_et().expect("should produce datetime");
        use chrono::Timelike;
        assert_eq!(dt.hour(), 0);
        assert_eq!(dt.minute(), 0);
    }

    // ── NDJSON multi-line parsing simulation ───────────────────────────────────

    #[test]
    fn test_ndjson_parse_multiple_ohlc_rows() {
        // Simulate what `execute()` does: parse each newline-delimited JSON object.
        let ndjson = r#"{"symbol":"AAPL","expiration":"20240119","strike":185000.0,"right":"C","timestamp":"2024-01-15T10:00:00.000","date":"20240115","ms_of_day":36000000,"open":2.50,"high":3.10,"low":2.40,"close":2.90,"volume":1500.0,"count":42}
{"symbol":"AAPL","expiration":"20240119","strike":185000.0,"right":"C","timestamp":"2024-01-15T11:00:00.000","date":"20240115","ms_of_day":39600000,"open":2.90,"high":3.00,"low":2.80,"close":2.95,"volume":800.0,"count":20}
"#;

        let rows: Vec<V3OptionOhlc> = ndjson
            .lines()
            .filter(|l| !l.trim().is_empty())
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect();

        assert_eq!(rows.len(), 2);
        assert!((rows[0].open - 2.50).abs() < f64::EPSILON);
        assert!((rows[1].open - 2.90).abs() < f64::EPSILON);
        assert_eq!(rows[0].ms_of_day, 36_000_000);
        assert_eq!(rows[1].ms_of_day, 39_600_000);
    }

    #[test]
    fn test_ndjson_skips_malformed_rows() {
        // Malformed rows should be silently dropped (mirrors execute() behaviour).
        let ndjson = r#"{"open":1.0,"high":1.5,"low":0.9,"close":1.2,"volume":100.0}
THIS IS NOT JSON
{"open":2.0,"high":2.5,"low":1.9,"close":2.2,"volume":200.0}
"#;

        let rows: Vec<V3OptionOhlc> = ndjson
            .lines()
            .filter(|l| !l.trim().is_empty())
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect();

        assert_eq!(rows.len(), 2, "malformed middle line should be skipped");
    }
}
