//! CBOE VIX daily OHLC custom data source plugin for rlean.
//!
//! rlean reads this plugin as native Parquet only. The plugin owns any upstream
//! wire conversion and persists canonical files under:
//! `{RLEAN_DATA_DIR}/alternative/cboe_vix/vix/daily/{YYYY}/{MM}/{DD}/1600.parquet`.

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::OnceLock;

use arrow_array::{Float64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use chrono::{Datelike, NaiveDate};
use lean_data::custom::{CustomDataConfig, CustomDataPoint, CustomDataQuery, CustomParquetSource};
use lean_data_providers::ICustomDataSource;
use lean_plugin::{rlean_plugin, PluginKind};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use rust_decimal::Decimal;
use std::sync::Arc;

pub const CBOE_VIX_URL: &str =
    "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv";

static POPULATE_RESULT: OnceLock<Result<(), String>> = OnceLock::new();

pub struct CboeVixDataSource {
    data_dir: PathBuf,
}

impl CboeVixDataSource {
    pub fn new() -> Self {
        CboeVixDataSource {
            data_dir: get_data_dir(),
        }
    }
}

impl Default for CboeVixDataSource {
    fn default() -> Self {
        Self::new()
    }
}

impl ICustomDataSource for CboeVixDataSource {
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
        _ticker: &str,
        date: NaiveDate,
        _config: &CustomDataConfig,
        _query: &CustomDataQuery,
    ) -> Option<CustomParquetSource> {
        match POPULATE_RESULT.get_or_init(|| populate_parquet_cache(&self.data_dir)) {
            Ok(()) => {}
            Err(error) => {
                eprintln!("[cboe_vix] {error}");
                return None;
            }
        }

        let path = vix_path(&self.data_dir, date);
        path.exists().then(|| CustomParquetSource {
            paths: vec![path.to_string_lossy().into_owned()],
            time_column: None,
            time_format: None,
            time_zone: None,
            symbol_column: None,
            value_column: Some("close".to_string()),
        })
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

fn get_data_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("RLEAN_DATA_DIR") {
        return PathBuf::from(dir);
    }
    if let Ok(home) = std::env::var("HOME") {
        let path = PathBuf::from(home).join(".rlean").join("data");
        if path.exists() {
            return path;
        }
    }
    PathBuf::from("data")
}

fn vix_path(data_dir: &Path, date: NaiveDate) -> PathBuf {
    data_dir
        .join("alternative")
        .join("cboe_vix")
        .join("vix")
        .join("daily")
        .join(format!("{:04}", date.year()))
        .join(format!("{:02}", date.month()))
        .join(format!("{:02}", date.day()))
        .join("1600.parquet")
}

fn populate_parquet_cache(data_dir: &Path) -> Result<(), String> {
    let text = fetch_history_text().map_err(|error| format!("fetch failed: {error}"))?;
    for row in text.lines().filter_map(parse_history_row) {
        let path = vix_path(data_dir, row.date);
        if path.exists() {
            continue;
        }
        write_vix_row(&path, &row).map_err(|error| format!("write {}: {error}", path.display()))?;
    }
    Ok(())
}

fn fetch_history_text() -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let response = reqwest::get(CBOE_VIX_URL).await?.error_for_status()?;
        Ok(response.text().await?)
    })
}

#[derive(Debug, Clone, PartialEq)]
struct VixRow {
    date: NaiveDate,
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
}

fn parse_history_row(line: &str) -> Option<VixRow> {
    let line = line.trim();
    if line.is_empty() || line.starts_with("DATE") {
        return None;
    }

    let mut parts = line.splitn(5, ',');
    let date = NaiveDate::parse_from_str(parts.next()?.trim(), "%m/%d/%Y").ok()?;
    let open = Decimal::from_str(parts.next()?.trim()).ok()?;
    let high = Decimal::from_str(parts.next()?.trim()).ok()?;
    let low = Decimal::from_str(parts.next()?.trim()).ok()?;
    let close = Decimal::from_str(parts.next()?.trim()).ok()?;

    Some(VixRow {
        date,
        open,
        high,
        low,
        close,
    })
}

fn write_vix_row(path: &Path, row: &VixRow) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("open", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
    ]));
    let decimal_to_f64 = |value: Decimal| value.to_string().parse::<f64>().unwrap_or(0.0);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Float64Array::from(vec![decimal_to_f64(row.open)])),
            Arc::new(Float64Array::from(vec![decimal_to_f64(row.high)])),
            Arc::new(Float64Array::from(vec![decimal_to_f64(row.low)])),
            Arc::new(Float64Array::from(vec![decimal_to_f64(row.close)])),
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
        let row = parse_history_row("03/16/2020,82.69,85.47,74.53,82.69").unwrap();
        assert_eq!(row.date, date(2020, 3, 16));
        assert_eq!(row.high, Decimal::from_str("85.47").unwrap());
        assert_eq!(row.low, Decimal::from_str("74.53").unwrap());
    }

    #[test]
    fn vix_path_uses_alternative_layout() {
        assert_eq!(
            vix_path(Path::new("/data"), date(2024, 1, 15)),
            PathBuf::from("/data/alternative/cboe_vix/vix/daily/2024/01/15/1600.parquet")
        );
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
