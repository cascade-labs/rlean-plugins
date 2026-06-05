//! CBOE VIX-family daily OHLC custom data source plugin for rlean.
//!
//! rlean reads this plugin as native Parquet only. The plugin owns any upstream
//! wire conversion and persists canonical files under:
//! `{data_root}/alternative/cboe_vix/{ticker}/daily/{YYYY}/{MM}/{DD}/1600.parquet`.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::OnceLock;

use arrow_array::{Float64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use chrono::{Datelike, NaiveDate};
use lean_data::custom::{CustomDataConfig, CustomDataPoint, CustomDataQuery, CustomParquetSource};
use lean_data_providers::{CustomDataContext, ICustomDataSource};
use lean_plugin::{rlean_plugin, PluginKind};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use rust_decimal::Decimal;
use std::sync::Arc;

pub const CBOE_INDEX_BASE_URL: &str = "https://cdn.cboe.com/api/global/us_indices/daily_prices";
pub const CBOE_FUTURES_HISTORY_URL: &str =
    "https://ww2.cboe.com/us/futures/market_statistics/historical_data/";
pub const CBOE_FUTURES_CDN_PREFIX: &str = "https://cdn.cboe.com/data";
pub const CBOE_VX30_MIN_EXPIRY_YEAR: i32 = 2019;

static POPULATE_VIX_RESULT: OnceLock<Result<(), String>> = OnceLock::new();
static POPULATE_VVIX_RESULT: OnceLock<Result<(), String>> = OnceLock::new();
static POPULATE_VIX3M_RESULT: OnceLock<Result<(), String>> = OnceLock::new();
static POPULATE_VX30_RESULT: OnceLock<Result<(), String>> = OnceLock::new();

pub struct CboeVixDataSource {
    data_dir: PathBuf,
}

impl CboeVixDataSource {
    pub fn new() -> Self {
        CboeVixDataSource {
            data_dir: PathBuf::new(),
        }
    }
}

impl Default for CboeVixDataSource {
    fn default() -> Self {
        Self::new()
    }
}

impl ICustomDataSource for CboeVixDataSource {
    fn initialize(&mut self, context: &CustomDataContext) {
        self.data_dir = context.data_root().to_path_buf();
    }

    fn name(&self) -> &str {
        "cboe_vix"
    }

    fn get_source(
        &self,
        _ticker: &str,
        _date: NaiveDate,
        _config: &CustomDataConfig,
    ) -> Option<lean_data::custom::CustomDataSource> {
        None
    }

    fn get_parquet_source(
        &self,
        ticker: &str,
        date: NaiveDate,
        _config: &CustomDataConfig,
        _query: &CustomDataQuery,
    ) -> Option<CustomParquetSource> {
        if self.data_dir.as_os_str().is_empty() {
            eprintln!("[cboe_vix] data root was not initialized by rlean");
            return None;
        }

        let ticker = normalize_ticker(ticker);
        let path = series_path(&self.data_dir, &ticker, date);
        if path.exists() {
            return Some(parquet_source(path));
        }
        if series_populated(&self.data_dir, &ticker) {
            return None;
        }

        let populate_result = match ticker.as_str() {
            "VIX" => POPULATE_VIX_RESULT
                .get_or_init(|| populate_index_parquet_cache(&self.data_dir, "VIX")),
            "VVIX" => POPULATE_VVIX_RESULT
                .get_or_init(|| populate_index_parquet_cache(&self.data_dir, "VVIX")),
            "VIX3M" => POPULATE_VIX3M_RESULT
                .get_or_init(|| populate_index_parquet_cache(&self.data_dir, "VIX3M")),
            "VX30" => {
                POPULATE_VX30_RESULT.get_or_init(|| populate_vx30_parquet_cache(&self.data_dir))
            }
            _ => {
                eprintln!(
                    "[cboe_vix] unsupported ticker {ticker}; supported: VIX, VVIX, VIX3M, VX30"
                );
                return None;
            }
        };

        match populate_result {
            Ok(()) => {}
            Err(error) => {
                eprintln!("[cboe_vix] {error}");
                return None;
            }
        }

        path.exists().then(|| parquet_source(path))
    }

    fn is_parquet_native(&self) -> bool {
        true
    }

    fn default_resolution(&self) -> lean_core::Resolution {
        lean_core::Resolution::Daily
    }

    fn reader(
        &self,
        _line: &str,
        _date: NaiveDate,
        _config: &CustomDataConfig,
    ) -> Option<CustomDataPoint> {
        None
    }

    fn is_full_history_source(&self) -> bool {
        false
    }

    fn read_history_line(
        &self,
        _line: &str,
        _config: &CustomDataConfig,
    ) -> Option<CustomDataPoint> {
        None
    }
}

fn normalize_ticker(ticker: &str) -> String {
    ticker.trim().to_ascii_uppercase()
}

fn series_path(data_dir: &Path, ticker: &str, date: NaiveDate) -> PathBuf {
    series_dir(data_dir, ticker)
        .join("daily")
        .join(format!("{:04}", date.year()))
        .join(format!("{:02}", date.month()))
        .join(format!("{:02}", date.day()))
        .join("1600.parquet")
}

fn series_dir(data_dir: &Path, ticker: &str) -> PathBuf {
    data_dir
        .join("alternative")
        .join("cboe_vix")
        .join(ticker.to_ascii_lowercase())
}

fn populated_marker_path(data_dir: &Path, ticker: &str) -> PathBuf {
    series_dir(data_dir, ticker).join(".populated")
}

fn series_populated(data_dir: &Path, ticker: &str) -> bool {
    populated_marker_path(data_dir, ticker).exists()
        && series_dir(data_dir, ticker).join("daily").exists()
}

fn mark_series_populated(data_dir: &Path, ticker: &str) -> anyhow::Result<()> {
    let dir = series_dir(data_dir, ticker);
    std::fs::create_dir_all(&dir)?;
    std::fs::write(populated_marker_path(data_dir, ticker), b"ok\n")?;
    Ok(())
}

fn parquet_source(path: PathBuf) -> CustomParquetSource {
    CustomParquetSource {
        paths: vec![path.to_string_lossy().into_owned()],
        time_column: None,
        time_format: None,
        time_zone: None,
        end_time_offset_nanos: None,
        symbol_column: None,
        value_column: Some("close".to_string()),
    }
}

fn populate_index_parquet_cache(data_dir: &Path, ticker: &str) -> Result<(), String> {
    let url = format!("{CBOE_INDEX_BASE_URL}/{ticker}_History.csv");
    let text = fetch_text(&url).map_err(|error| format!("{ticker} fetch failed: {error}"))?;
    let mut rows = 0;
    for row in text.lines().filter_map(parse_index_history_row) {
        rows += 1;
        let path = series_path(data_dir, ticker, row.date);
        if path.exists() {
            continue;
        }
        write_ohlc_row(
            &path,
            decimal_to_f64(row.open),
            decimal_to_f64(row.high),
            decimal_to_f64(row.low),
            decimal_to_f64(row.close),
        )
        .map_err(|error| format!("write {}: {error}", path.display()))?;
    }
    if rows == 0 {
        return Err(format!(
            "{ticker} history parse produced zero rows from {url}"
        ));
    }
    mark_series_populated(data_dir, ticker)
        .map_err(|error| format!("write {ticker} populated marker: {error}"))?;
    Ok(())
}

fn populate_vx30_parquet_cache(data_dir: &Path) -> Result<(), String> {
    let html = fetch_text(CBOE_FUTURES_HISTORY_URL)
        .map_err(|error| format!("VX history page fetch failed: {error}"))?;
    let urls = extract_vx_history_urls(&html);
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

    for (date, mut curve) in settlements {
        let path = series_path(data_dir, "VX30", date);
        if path.exists() {
            continue;
        }
        let Some(vx30) = interpolate_vx30(&mut curve) else {
            continue;
        };
        write_ohlc_row(&path, vx30, vx30, vx30, vx30)
            .map_err(|error| format!("write {}: {error}", path.display()))?;
    }

    mark_series_populated(data_dir, "VX30")
        .map_err(|error| format!("write VX30 populated marker: {error}"))?;
    Ok(())
}

fn fetch_text(url: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let response = reqwest::get(url).await?.error_for_status()?;
        Ok(response.text().await?)
    })
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

fn write_ohlc_row(path: &Path, open: f64, high: f64, low: f64, close: f64) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("open", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Float64Array::from(vec![open])),
            Arc::new(Float64Array::from(vec![high])),
            Arc::new(Float64Array::from(vec![low])),
            Arc::new(Float64Array::from(vec![close])),
        ],
    )?;

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_statistics_enabled(EnabledStatistics::Page)
        .build();
    let file = std::fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn decimal_to_f64(value: Decimal) -> f64 {
    value.to_string().parse::<f64>().unwrap_or(0.0)
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
    fn series_path_uses_alternative_layout() {
        assert_eq!(
            series_path(Path::new("/data"), "VIX", date(2024, 1, 15)),
            PathBuf::from("/data/alternative/cboe_vix/vix/daily/2024/01/15/1600.parquet")
        );
        assert_eq!(
            series_path(Path::new("/data"), "VIX3M", date(2024, 1, 15)),
            PathBuf::from("/data/alternative/cboe_vix/vix3m/daily/2024/01/15/1600.parquet")
        );
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
    fn plugin_is_parquet_native() {
        assert!(CboeVixDataSource::new().is_parquet_native());
        assert!(CboeVixDataSource::new()
            .get_source(
                "VIX",
                date(2024, 1, 2),
                &CustomDataConfig {
                    ticker: "VIX".to_string(),
                    source_type: "cboe_vix".to_string(),
                    resolution: lean_core::Resolution::Daily,
                    properties: HashMap::new(),
                    query: CustomDataQuery::default(),
                }
            )
            .is_none());
    }
}
