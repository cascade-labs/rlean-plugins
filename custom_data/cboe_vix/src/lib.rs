//! CBOE VIX-family daily OHLC custom data source plugin for rlean.
//!
//! Historical CBOE VIX custom data is stored in rlean's Iceberg custom data table.
//! This plugin no longer exposes or owns parquet file layout.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::str::FromStr;

use chrono::{Datelike, NaiveDate};
use lean_core::{DateTime, TimeSpan};
use lean_data::custom::{CustomDataConfig, CustomDataPoint, CustomDataSource, CustomDataTransport};
use lean_data_providers::{CustomDataContext, ICustomDataSource};
use lean_plugin::{rlean_plugin, PluginKind};
use rust_decimal::Decimal;

pub const CBOE_INDEX_BASE_URL: &str = "https://cdn.cboe.com/api/global/us_indices/daily_prices";
pub const CBOE_FUTURES_HISTORY_URL: &str =
    "https://ww2.cboe.com/us/futures/market_statistics/historical_data/";
pub const CBOE_FUTURES_CDN_PREFIX: &str = "https://cdn.cboe.com/data";
pub const CBOE_VX30_MIN_EXPIRY_YEAR: i32 = 2019;

pub struct CboeVixDataSource;

impl CboeVixDataSource {
    pub fn new() -> Self {
        CboeVixDataSource
    }
}

impl Default for CboeVixDataSource {
    fn default() -> Self {
        Self::new()
    }
}

impl ICustomDataSource for CboeVixDataSource {
    fn initialize(&mut self, _context: &CustomDataContext) {}

    fn name(&self) -> &str {
        "cboe_vix"
    }

    fn get_source(
        &self,
        ticker: &str,
        _date: NaiveDate,
        _config: &CustomDataConfig,
    ) -> Option<CustomDataSource> {
        let ticker = normalize_ticker(ticker);
        if ticker == "VX30" {
            return None;
        }
        Some(CustomDataSource {
            uri: format!("{CBOE_INDEX_BASE_URL}/{ticker}_History.csv"),
            transport: CustomDataTransport::Http,
            format: lean_data::custom::CustomDataFormat::Csv,
        })
    }

    fn default_resolution(&self) -> lean_core::Resolution {
        lean_core::Resolution::Daily
    }

    fn reader(
        &self,
        line: &str,
        _date: NaiveDate,
        _config: &CustomDataConfig,
    ) -> Option<CustomDataPoint> {
        parse_index_history_row(line).map(|row| custom_point(row.date, row.close))
    }

    fn is_full_history_source(&self) -> bool {
        true
    }

    fn read_history_line(&self, line: &str, _config: &CustomDataConfig) -> Option<CustomDataPoint> {
        self.reader(line, NaiveDate::MIN, _config)
    }

    fn history(
        &self,
        ticker: &str,
        config: &CustomDataConfig,
    ) -> Option<Result<Vec<CustomDataPoint>, String>> {
        let ticker = normalize_ticker(ticker);
        if ticker == "VX30" {
            Some(fetch_vx30_history(config))
        } else {
            let source = self.get_source(&ticker, NaiveDate::MIN, config)?;
            Some(
                fetch_text(&source.uri)
                    .map_err(|error| format!("{ticker} fetch failed: {error}"))
                    .map(|text| {
                        text.lines()
                            .filter_map(|line| self.read_history_line(line, config))
                            .filter(|point| custom_point_in_query(point, config))
                            .collect()
                    }),
            )
        }
    }
}

fn normalize_ticker(ticker: &str) -> String {
    ticker.trim().to_ascii_uppercase()
}

fn fetch_vx30_history(config: &CustomDataConfig) -> Result<Vec<CustomDataPoint>, String> {
    let html = fetch_text(CBOE_FUTURES_HISTORY_URL)
        .map_err(|error| format!("VX history page fetch failed: {error}"))?;
    let (start, end) = query_dates(config);
    let urls = extract_vx_history_urls(&html)
        .into_iter()
        .filter(|(expiry, _)| expiry_in_query_window(*expiry, start, end))
        .collect::<Vec<_>>();
    if urls.is_empty() {
        return Err("VX history page did not expose any VX contract CSV links".to_string());
    }

    let mut settlements: BTreeMap<NaiveDate, Vec<(i64, f64)>> = BTreeMap::new();
    for (idx, (expiry, url)) in urls.iter().enumerate() {
        let text = fetch_text(url).map_err(|error| {
            format!("VX contract fetch failed expiry={expiry} url={url}: {error}")
        })?;
        if idx > 0 && idx % 50 == 0 {
            eprintln!(
                "[cboe_vix] VX30 fetched {idx}/{} contract histories",
                urls.len()
            );
        }
        for row in text
            .lines()
            .filter_map(|line| parse_vx_contract_row(line, *expiry))
        {
            let dte = row.expiry.signed_duration_since(row.trade_date).num_days();
            if dte <= 0 || row.settle <= 0.0 || !row.settle.is_finite() {
                continue;
            }
            settlements
                .entry(row.trade_date)
                .or_default()
                .push((dte, row.settle));
        }
    }

    Ok(settlements
        .into_iter()
        .filter_map(|(date, mut curve)| {
            if !date_in_query_window(date, start, end) {
                return None;
            }
            Decimal::from_str(&interpolate_vx30(&mut curve)?.to_string())
                .ok()
                .map(|value| custom_point(date, value))
        })
        .collect())
}

fn custom_point_in_query(point: &CustomDataPoint, config: &CustomDataConfig) -> bool {
    let (start, end) = query_dates(config);
    date_in_query_window(point.time, start, end)
}

fn query_dates(config: &CustomDataConfig) -> (Option<NaiveDate>, Option<NaiveDate>) {
    let start = config
        .query
        .start_date
        .or_else(|| config.query.start_time.map(|time| time.date_utc()));
    let end = config
        .query
        .end_date
        .or_else(|| config.query.end_time.map(|time| time.date_utc()));
    (start, end)
}

fn date_in_query_window(date: NaiveDate, start: Option<NaiveDate>, end: Option<NaiveDate>) -> bool {
    start.map_or(true, |start| date >= start) && end.map_or(true, |end| date <= end)
}

fn expiry_in_query_window(
    expiry: NaiveDate,
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
) -> bool {
    let min_expiry = start.map(|date| date + chrono::Duration::days(1));
    let max_expiry = end.map(|date| date + chrono::Duration::days(120));
    date_in_query_window(expiry, min_expiry, max_expiry)
}

fn fetch_text(url: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let response = reqwest::blocking::get(url)?.error_for_status()?;
    Ok(response.text()?)
}

#[derive(Debug, Clone, PartialEq)]
struct IndexRow {
    date: NaiveDate,
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
}

fn parse_index_history_row(line: &str) -> Option<IndexRow> {
    let line = line.trim();
    if line.is_empty() || line.starts_with("DATE") {
        return None;
    }

    let parts: Vec<&str> = line.split(',').collect();
    let date = NaiveDate::parse_from_str(parts.first()?.trim(), "%m/%d/%Y").ok()?;
    let (open, high, low, close) = if parts.len() == 2 {
        let value = Decimal::from_str(parts.get(1)?.trim()).ok()?;
        (value, value, value, value)
    } else if parts.len() >= 5 {
        (
            Decimal::from_str(parts.get(1)?.trim()).ok()?,
            Decimal::from_str(parts.get(2)?.trim()).ok()?,
            Decimal::from_str(parts.get(3)?.trim()).ok()?,
            Decimal::from_str(parts.get(4)?.trim()).ok()?,
        )
    } else {
        return None;
    };

    Some(IndexRow {
        date,
        open,
        high,
        low,
        close,
    })
}

fn custom_point(date: NaiveDate, value: Decimal) -> CustomDataPoint {
    let time = DateTime::from(
        date.and_hms_opt(16, 0, 0)
            .expect("valid CBOE history timestamp")
            .and_utc(),
    );
    CustomDataPoint {
        time: date,
        end_time: Some(time + TimeSpan::ZERO),
        value,
        fields: HashMap::new(),
    }
}

#[derive(Debug, Clone, PartialEq)]
struct VxContractRow {
    trade_date: NaiveDate,
    expiry: NaiveDate,
    settle: f64,
}

fn extract_vx_history_urls(html: &str) -> Vec<(NaiveDate, String)> {
    let marker = "/us/futures/market_statistics/historical_data/VX/VX_";
    let mut urls = Vec::new();
    let mut seen = HashSet::new();
    let mut offset = 0;

    while let Some(relative_start) = html[offset..].find(marker) {
        let start = offset + relative_start;
        let Some(relative_end) = html[start..].find(".csv") else {
            break;
        };
        let end = start + relative_end + 4;
        let path = &html[start..end];
        offset = end;

        if !seen.insert(path.to_string()) {
            continue;
        }

        let Some(date_start) = path.rfind("VX_") else {
            continue;
        };
        let date_text = &path[date_start + 3..path.len() - 4];
        let Ok(expiry) = NaiveDate::parse_from_str(date_text, "%Y-%m-%d") else {
            continue;
        };
        if expiry.year() < CBOE_VX30_MIN_EXPIRY_YEAR {
            continue;
        }
        urls.push((expiry, format!("{CBOE_FUTURES_CDN_PREFIX}{path}")));
    }

    urls.sort_by_key(|(expiry, _)| *expiry);
    urls
}

fn parse_vx_contract_row(line: &str, expiry: NaiveDate) -> Option<VxContractRow> {
    let line = line.trim();
    if line.is_empty() || line.starts_with("Trade Date") {
        return None;
    }

    let parts: Vec<&str> = line.split(',').collect();
    if parts.len() < 7 {
        return None;
    }
    let trade_date = NaiveDate::parse_from_str(parts[0].trim(), "%Y-%m-%d").ok()?;
    let settle_text = parts[6].trim().trim_end_matches('*');
    let settle = settle_text.parse::<f64>().ok()?;
    Some(VxContractRow {
        trade_date,
        expiry,
        settle,
    })
}

fn interpolate_vx30(curve: &mut [(i64, f64)]) -> Option<f64> {
    curve.sort_by_key(|(dte, _)| *dte);
    let mut lower = None;
    let mut upper = None;

    for (dte, settle) in curve.iter().copied() {
        if dte == 30 {
            return Some(settle);
        }
        if dte < 30 {
            lower = Some((dte, settle));
        } else if dte > 30 {
            upper = Some((dte, settle));
            break;
        }
    }

    let ((lower_dte, lower_settle), (upper_dte, upper_settle)) = (lower?, upper?);
    if upper_dte == lower_dte {
        return None;
    }
    let weight = (30 - lower_dte) as f64 / (upper_dte - lower_dte) as f64;
    Some(lower_settle + weight * (upper_settle - lower_settle))
}

rlean_plugin! {
    name    = "cboe_vix",
    version = "0.1.0",
    kind    = PluginKind::CustomData,
}

#[no_mangle]
pub extern "C" fn rlean_custom_data_factory() -> *mut () {
    let source: Box<dyn ICustomDataSource> = Box::new(CboeVixDataSource::new());
    Box::into_raw(Box::new(source)) as *mut ()
}

#[cfg(test)]
mod tests {
    use super::*;
    use lean_data::CustomDataQuery;
    use std::collections::HashMap;

    fn date(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    #[test]
    fn parse_history_row_parses_ohlc() {
        let row = parse_index_history_row("03/16/2020,82.69,85.47,74.53,82.69").unwrap();
        assert_eq!(row.date, date(2020, 3, 16));
        assert_eq!(row.high, Decimal::from_str("85.47").unwrap());
        assert_eq!(row.low, Decimal::from_str("74.53").unwrap());
    }

    #[test]
    fn parse_history_row_accepts_single_value_indexes() {
        let row = parse_index_history_row("03/16/2020,117.12").unwrap();
        assert_eq!(row.date, date(2020, 3, 16));
        assert_eq!(row.open, Decimal::from_str("117.12").unwrap());
        assert_eq!(row.high, Decimal::from_str("117.12").unwrap());
        assert_eq!(row.low, Decimal::from_str("117.12").unwrap());
        assert_eq!(row.close, Decimal::from_str("117.12").unwrap());
    }

    #[test]
    fn extract_vx_history_urls_builds_data_cdn_urls() {
        let html = r#"
            <a href="/us/futures/market_statistics/historical_data/VX/VX_2025-03-18.csv">VX</a>
            <a href="/us/futures/market_statistics/historical_data/VX/VX_2025-04-16.csv">VX</a>
        "#;
        let urls = extract_vx_history_urls(html);
        assert_eq!(urls.len(), 2);
        assert_eq!(urls[0].0, date(2025, 3, 18));
        assert_eq!(
            urls[0].1,
            "https://cdn.cboe.com/data/us/futures/market_statistics/historical_data/VX/VX_2025-03-18.csv"
        );
    }

    #[test]
    fn parse_vx_contract_row_reads_settlement() {
        let row = parse_vx_contract_row(
            "2024-06-24,H (Mar 2025),0.0000,18.4500,19.0000,0.0000,18.925,0,0,0,0",
            date(2025, 3, 18),
        )
        .unwrap();
        assert_eq!(row.trade_date, date(2024, 6, 24));
        assert_eq!(row.expiry, date(2025, 3, 18));
        assert_eq!(row.settle, 18.925);
    }

    #[test]
    fn interpolate_vx30_uses_bracketing_contracts() {
        let mut curve = vec![(20, 18.0), (40, 22.0)];
        assert_eq!(interpolate_vx30(&mut curve), Some(20.0));

        let mut exact_curve = vec![(25, 18.0), (30, 19.0), (40, 22.0)];
        assert_eq!(interpolate_vx30(&mut exact_curve), Some(19.0));
    }

    #[test]
    fn plugin_exposes_provider_sources_without_storage_layout() {
        assert_eq!(CboeVixDataSource::new().name(), "cboe_vix");
        let config = CustomDataConfig {
            ticker: "VIX".to_string(),
            source_type: "cboe_vix".to_string(),
            resolution: lean_core::Resolution::Daily,
            properties: HashMap::new(),
            query: CustomDataQuery::default(),
        };
        let source = CboeVixDataSource::new()
            .get_source("VIX", date(2024, 1, 2), &config)
            .unwrap();
        assert!(source.uri.ends_with("/VIX_History.csv"));
        assert!(CboeVixDataSource::new()
            .get_source("VX30", date(2024, 1, 2), &config)
            .is_none());
    }
}
