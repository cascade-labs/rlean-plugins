use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::{NaiveDate, TimeZone, Timelike, Utc};
use lean_core::{Market, NanosecondTimestamp, Resolution, SecurityType};
use lean_data::custom::{CustomDataConfig, CustomDataFormat, CustomDataPoint, CustomDataSource};
use lean_data_providers::{CustomDataContext, ICustomDataSource};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{json, Map, Value};

use crate::archive::{ArchiveBuckets, ArchiveCredentials, ArchiveRegions, S3ArchiveClient};
use crate::history_provider::{asset_contexts_key, HyperliquidInfoClient};

const DEFAULT_INFO_URL: &str = "https://api.hyperliquid.xyz/info";
const HEADER: &str = "time_ns,symbol,coin,security_type,market,universe,dex,source,is_historical,value,funding,open_interest,prev_day_px,day_ntl_vlm,premium,oracle_px,mark_px,mid_px,impact_bid_px,impact_ask_px,max_leverage,sz_decimals,index,base,quote";

pub struct HyperliquidUniverseDataSource {
    data_root: PathBuf,
    plugin_config: Map<String, Value>,
}

impl HyperliquidUniverseDataSource {
    pub fn new() -> Self {
        Self {
            data_root: PathBuf::new(),
            plugin_config: Map::new(),
        }
    }

    fn raw_path(&self, ticker: &str, date: NaiveDate) -> PathBuf {
        self.data_root
            .join("custom")
            .join("hyperliquid")
            .join(normalize_universe(ticker).to_ascii_lowercase())
            .join("raw")
            .join(format!("{}.csv", date.format("%Y%m%d")))
    }

    fn ensure_raw_file(
        &self,
        ticker: &str,
        date: NaiveDate,
        config: &CustomDataConfig,
    ) -> Result<Option<PathBuf>> {
        let path = self.raw_path(ticker, date);
        if path.exists() {
            return Ok(Some(path));
        }

        let universe = normalize_universe(
            config
                .properties
                .get("universe")
                .map(String::as_str)
                .unwrap_or(ticker),
        );
        let rows = self
            .load_rows(&universe, date, config.resolution, config)?
            .unwrap_or_default();
        if rows.is_empty() {
            return Ok(None);
        }

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let mut text = String::with_capacity(rows.len() * 256);
        text.push_str(HEADER);
        text.push('\n');
        for row in rows {
            text.push_str(&row.to_csv_line());
            text.push('\n');
        }
        std::fs::write(&path, text)
            .with_context(|| format!("failed to write {}", path.display()))?;
        Ok(Some(path))
    }

    fn load_rows(
        &self,
        universe: &str,
        date: NaiveDate,
        resolution: Resolution,
        config: &CustomDataConfig,
    ) -> Result<Option<Vec<UniverseRow>>> {
        match universe {
            "CRYPTO_PERP" => self.load_asset_ctx_rows(universe, None, date, resolution, config),
            "CRYPTO_SPOT" => self.load_spot_api_rows(universe, date, config).map(Some),
            value if value.starts_with("HIP3_") => {
                let dex = hip3_dex(value)?;
                let historical = self.load_asset_ctx_rows(universe, Some(&dex), date, resolution, config)?;
                if historical.as_ref().is_some_and(|rows| !rows.is_empty()) {
                    return Ok(historical);
                }
                self.load_perp_api_rows(universe, &dex, date, config).map(Some)
            }
            other => Err(anyhow::anyhow!(
                "unsupported Hyperliquid universe '{other}'. Supported: CRYPTO_PERP, CRYPTO_SPOT, HIP3_<dex>"
            )),
        }
    }

    fn load_asset_ctx_rows(
        &self,
        universe: &str,
        dex_filter: Option<&str>,
        date: NaiveDate,
        resolution: Resolution,
        config: &CustomDataConfig,
    ) -> Result<Option<Vec<UniverseRow>>> {
        let archive = self.archive_client(config);
        let key = asset_contexts_key(date);
        let text = block_on_archive(archive.market_text(&key))?.with_context(|| {
            format!(
                "Hyperliquid asset_ctxs archive missing s3://{}/{}",
                archive.buckets().market,
                key
            )
        })?;
        let rows = parse_asset_ctx_universe_rows(&text, universe, dex_filter, date, resolution)?;
        Ok(Some(rows))
    }

    fn load_perp_api_rows(
        &self,
        universe: &str,
        dex: &str,
        date: NaiveDate,
        config: &CustomDataConfig,
    ) -> Result<Vec<UniverseRow>> {
        let info = self.info_client(config);
        let response = info.meta_and_asset_ctxs(Some(dex))?;
        parse_meta_and_asset_ctx_rows(&response, universe, dex, date)
    }

    fn load_spot_api_rows(
        &self,
        universe: &str,
        date: NaiveDate,
        config: &CustomDataConfig,
    ) -> Result<Vec<UniverseRow>> {
        let info = self.info_client(config);
        let response = info.spot_meta_and_asset_ctxs()?;
        parse_spot_meta_and_asset_ctx_rows(&response, universe, date)
    }

    fn archive_client(&self, config: &CustomDataConfig) -> S3ArchiveClient {
        S3ArchiveClient::new(
            config_path(config, &self.plugin_config, "cache_dir")
                .or_else(|| std::env::var("HYPERLIQUID_ARCHIVE_CACHE_DIR").ok())
                .map(PathBuf::from),
            ArchiveBuckets {
                market: config_string(config, &self.plugin_config, "market_bucket")
                    .or_else(|| std::env::var("HYPERLIQUID_MARKET_BUCKET").ok())
                    .unwrap_or_else(|| "hyperliquid-archive".to_string()),
                fills: config_string(config, &self.plugin_config, "fills_bucket")
                    .or_else(|| std::env::var("HYPERLIQUID_FILLS_BUCKET").ok())
                    .unwrap_or_else(|| "hl-mainnet-node-data".to_string()),
            },
            config_string(config, &self.plugin_config, "request_payer")
                .or_else(|| std::env::var("HYPERLIQUID_REQUEST_PAYER").ok())
                .unwrap_or_else(|| "requester".to_string()),
            ArchiveRegions {
                market: config_string(config, &self.plugin_config, "market_region")
                    .or_else(|| std::env::var("HYPERLIQUID_MARKET_REGION").ok())
                    .unwrap_or_else(|| "us-east-1".to_string()),
                fills: config_string(config, &self.plugin_config, "fills_region")
                    .or_else(|| std::env::var("HYPERLIQUID_FILLS_REGION").ok())
                    .unwrap_or_else(|| "ap-northeast-1".to_string()),
            },
            archive_credentials(config, &self.plugin_config),
        )
    }

    fn info_client(&self, config: &CustomDataConfig) -> HyperliquidInfoClient {
        HyperliquidInfoClient::new(
            config_string(config, &self.plugin_config, "info_url")
                .or_else(|| std::env::var("HYPERLIQUID_INFO_URL").ok())
                .unwrap_or_else(|| DEFAULT_INFO_URL.to_string()),
        )
    }
}

impl Default for HyperliquidUniverseDataSource {
    fn default() -> Self {
        Self::new()
    }
}

impl ICustomDataSource for HyperliquidUniverseDataSource {
    fn initialize(&mut self, context: &CustomDataContext) {
        self.data_root = context.data_root().to_path_buf();
        self.plugin_config = context.plugin_config().clone();
    }

    fn name(&self) -> &str {
        "hyperliquid"
    }

    fn get_source(
        &self,
        ticker: &str,
        date: NaiveDate,
        config: &CustomDataConfig,
    ) -> Option<CustomDataSource> {
        match self.ensure_raw_file(ticker, date, config) {
            Ok(Some(path)) => Some(CustomDataSource {
                uri: path.to_string_lossy().into_owned(),
                transport: lean_data::custom::CustomDataTransport::LocalFile,
                format: CustomDataFormat::Csv,
            }),
            Ok(None) => None,
            Err(error) => {
                eprintln!("[hyperliquid] universe source failed for {ticker} {date}: {error:#}");
                None
            }
        }
    }

    fn reader(
        &self,
        line: &str,
        date: NaiveDate,
        _config: &CustomDataConfig,
    ) -> Option<CustomDataPoint> {
        if line.starts_with("time_ns,") {
            return None;
        }
        let row = UniverseCsvRow::parse(line)?;
        let time = NanosecondTimestamp(row.time_ns);
        let mut fields = HashMap::new();
        fields.insert("symbol".to_string(), json!(row.symbol));
        fields.insert("coin".to_string(), json!(row.coin));
        fields.insert("security_type".to_string(), json!(row.security_type));
        fields.insert("market".to_string(), json!(row.market));
        fields.insert("universe".to_string(), json!(row.universe));
        fields.insert("dex".to_string(), json!(row.dex));
        fields.insert("source".to_string(), json!(row.source));
        fields.insert("is_historical".to_string(), json!(row.is_historical));
        fields.insert("funding".to_string(), json!(row.funding));
        fields.insert("open_interest".to_string(), json!(row.open_interest));
        fields.insert("prev_day_px".to_string(), json!(row.prev_day_px));
        fields.insert("day_ntl_vlm".to_string(), json!(row.day_ntl_vlm));
        fields.insert("premium".to_string(), json!(row.premium));
        fields.insert("oracle_px".to_string(), json!(row.oracle_px));
        fields.insert("mark_px".to_string(), json!(row.mark_px));
        fields.insert("mid_px".to_string(), json!(row.mid_px));
        fields.insert("impact_bid_px".to_string(), json!(row.impact_bid_px));
        fields.insert("impact_ask_px".to_string(), json!(row.impact_ask_px));
        fields.insert("max_leverage".to_string(), json!(row.max_leverage));
        fields.insert("sz_decimals".to_string(), json!(row.sz_decimals));
        fields.insert("index".to_string(), json!(row.index));
        fields.insert("base".to_string(), json!(row.base));
        fields.insert("quote".to_string(), json!(row.quote));

        Some(CustomDataPoint {
            time: date,
            end_time: Some(time),
            value: decimal_from_optional(&row.value),
            fields,
        })
    }

    fn default_resolution(&self) -> Resolution {
        Resolution::Hour
    }
}

#[derive(Debug, Clone)]
struct UniverseRow {
    time_ns: i64,
    symbol: String,
    coin: String,
    security_type: SecurityType,
    market: String,
    universe: String,
    dex: String,
    source: String,
    is_historical: bool,
    value: Option<Decimal>,
    funding: Option<Decimal>,
    open_interest: Option<Decimal>,
    prev_day_px: Option<Decimal>,
    day_ntl_vlm: Option<Decimal>,
    premium: Option<Decimal>,
    oracle_px: Option<Decimal>,
    mark_px: Option<Decimal>,
    mid_px: Option<Decimal>,
    impact_bid_px: Option<Decimal>,
    impact_ask_px: Option<Decimal>,
    max_leverage: Option<i64>,
    sz_decimals: Option<i64>,
    index: Option<i64>,
    base: Option<String>,
    quote: Option<String>,
}

impl UniverseRow {
    fn to_csv_line(&self) -> String {
        [
            self.time_ns.to_string(),
            csv_escape(&self.symbol),
            csv_escape(&self.coin),
            self.security_type.to_string(),
            csv_escape(&self.market),
            csv_escape(&self.universe),
            csv_escape(&self.dex),
            csv_escape(&self.source),
            self.is_historical.to_string(),
            decimal_to_string(self.value),
            decimal_to_string(self.funding),
            decimal_to_string(self.open_interest),
            decimal_to_string(self.prev_day_px),
            decimal_to_string(self.day_ntl_vlm),
            decimal_to_string(self.premium),
            decimal_to_string(self.oracle_px),
            decimal_to_string(self.mark_px),
            decimal_to_string(self.mid_px),
            decimal_to_string(self.impact_bid_px),
            decimal_to_string(self.impact_ask_px),
            option_i64_to_string(self.max_leverage),
            option_i64_to_string(self.sz_decimals),
            option_i64_to_string(self.index),
            csv_escape(self.base.as_deref().unwrap_or("")),
            csv_escape(self.quote.as_deref().unwrap_or("")),
        ]
        .join(",")
    }
}

#[derive(Debug, Deserialize)]
struct AssetContextCsvRow {
    time: String,
    coin: String,
    funding: String,
    open_interest: String,
    prev_day_px: String,
    day_ntl_vlm: String,
    premium: String,
    oracle_px: String,
    mark_px: String,
    mid_px: String,
    impact_bid_px: String,
    impact_ask_px: String,
}

fn parse_asset_ctx_universe_rows(
    text: &str,
    universe: &str,
    dex_filter: Option<&str>,
    date: NaiveDate,
    resolution: Resolution,
) -> Result<Vec<UniverseRow>> {
    if matches!(resolution, Resolution::Tick | Resolution::Second) {
        return Err(anyhow::anyhow!(
            "Hyperliquid asset_ctxs universe rows support Minute, Hour, and Daily resolutions"
        ));
    }

    let mut rows = Vec::new();
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(text.as_bytes());
    for row in reader.deserialize::<AssetContextCsvRow>() {
        let row = row.context("failed to parse Hyperliquid asset_ctxs universe CSV row")?;
        let time = parse_asset_ctx_time(&row.time)?;
        if time.date_naive() != date || !should_emit_time(time, resolution) {
            continue;
        }
        let source_coin = row.coin.trim().to_string();
        let dex = source_coin
            .split_once(':')
            .map(|(dex, _)| dex.to_string())
            .unwrap_or_default();
        if let Some(expected_dex) = dex_filter {
            if !dex.eq_ignore_ascii_case(expected_dex) {
                continue;
            }
        }
        let symbol = source_coin.to_ascii_uppercase();
        rows.push(UniverseRow {
            time_ns: time.timestamp_nanos_opt().unwrap_or_default(),
            symbol: symbol.clone(),
            coin: source_coin,
            security_type: SecurityType::CryptoFuture,
            market: Market::HYPERLIQUID.to_string(),
            universe: universe.to_string(),
            dex,
            source: "asset_ctxs".to_string(),
            is_historical: true,
            value: parse_decimal(&row.mid_px).or_else(|| parse_decimal(&row.mark_px)),
            funding: parse_decimal(&row.funding),
            open_interest: parse_decimal(&row.open_interest),
            prev_day_px: parse_decimal(&row.prev_day_px),
            day_ntl_vlm: parse_decimal(&row.day_ntl_vlm),
            premium: parse_decimal(&row.premium),
            oracle_px: parse_decimal(&row.oracle_px),
            mark_px: parse_decimal(&row.mark_px),
            mid_px: parse_decimal(&row.mid_px),
            impact_bid_px: parse_decimal(&row.impact_bid_px),
            impact_ask_px: parse_decimal(&row.impact_ask_px),
            max_leverage: None,
            sz_decimals: None,
            index: None,
            base: None,
            quote: Some("USDC".to_string()),
        });
    }
    Ok(rows)
}

fn parse_meta_and_asset_ctx_rows(
    response: &Value,
    universe: &str,
    dex: &str,
    date: NaiveDate,
) -> Result<Vec<UniverseRow>> {
    let array = response
        .as_array()
        .filter(|array| array.len() >= 2)
        .ok_or_else(|| {
            anyhow::anyhow!("Hyperliquid metaAndAssetCtxs response must be [meta, asset_ctxs]")
        })?;
    let meta = &array[0];
    let contexts = array[1].as_array().cloned().unwrap_or_default();
    let universe_rows = meta
        .get("universe")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            anyhow::anyhow!("Hyperliquid metaAndAssetCtxs response missing meta.universe")
        })?;
    let time_ns = date
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_nanos_opt()
        .unwrap_or_default();
    let mut rows = Vec::new();
    for (index, asset) in universe_rows.iter().enumerate() {
        let Some(name) = asset.get("name").and_then(Value::as_str) else {
            continue;
        };
        let context = contexts.get(index).unwrap_or(&Value::Null);
        let source_coin = name.trim().to_string();
        rows.push(UniverseRow {
            time_ns,
            symbol: source_coin.to_ascii_uppercase(),
            coin: source_coin.clone(),
            security_type: SecurityType::CryptoFuture,
            market: Market::HYPERLIQUID.to_string(),
            universe: universe.to_string(),
            dex: dex.to_string(),
            source: "info_api_current".to_string(),
            is_historical: false,
            value: decimal_field(context, "midPx").or_else(|| decimal_field(context, "markPx")),
            funding: decimal_field(context, "funding"),
            open_interest: decimal_field(context, "openInterest"),
            prev_day_px: decimal_field(context, "prevDayPx"),
            day_ntl_vlm: decimal_field(context, "dayNtlVlm"),
            premium: decimal_field(context, "premium"),
            oracle_px: decimal_field(context, "oraclePx"),
            mark_px: decimal_field(context, "markPx"),
            mid_px: decimal_field(context, "midPx"),
            impact_bid_px: decimal_field(context, "impactPxs")
                .or_else(|| decimal_field(context, "impactBidPx")),
            impact_ask_px: None,
            max_leverage: asset.get("maxLeverage").and_then(Value::as_i64),
            sz_decimals: asset.get("szDecimals").and_then(Value::as_i64),
            index: Some(index as i64),
            base: Some(source_coin),
            quote: Some("USDC".to_string()),
        });
    }
    Ok(rows)
}

fn parse_spot_meta_and_asset_ctx_rows(
    response: &Value,
    universe: &str,
    date: NaiveDate,
) -> Result<Vec<UniverseRow>> {
    let array = response
        .as_array()
        .filter(|array| array.len() >= 2)
        .ok_or_else(|| {
            anyhow::anyhow!("Hyperliquid spotMetaAndAssetCtxs response must be [meta, asset_ctxs]")
        })?;
    let meta = &array[0];
    let contexts = array[1].as_array().cloned().unwrap_or_default();
    let token_by_index: HashMap<i64, &Value> = meta
        .get("tokens")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|token| {
            token
                .get("index")
                .and_then(Value::as_i64)
                .map(|index| (index, token))
        })
        .collect();
    let universe_rows = meta
        .get("universe")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            anyhow::anyhow!("Hyperliquid spotMetaAndAssetCtxs response missing meta.universe")
        })?;
    let time_ns = date
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_nanos_opt()
        .unwrap_or_default();
    let mut rows = Vec::new();
    for (index, asset) in universe_rows.iter().enumerate() {
        let Some(name) = asset.get("name").and_then(Value::as_str) else {
            continue;
        };
        let context = contexts.get(index).unwrap_or(&Value::Null);
        let (base, quote) = spot_base_quote(asset, &token_by_index);
        rows.push(UniverseRow {
            time_ns,
            symbol: name.to_ascii_uppercase(),
            coin: name.to_ascii_uppercase(),
            security_type: SecurityType::Crypto,
            market: Market::HYPERLIQUID.to_string(),
            universe: universe.to_string(),
            dex: String::new(),
            source: "info_api_current".to_string(),
            is_historical: false,
            value: decimal_field(context, "midPx").or_else(|| decimal_field(context, "markPx")),
            funding: None,
            open_interest: None,
            prev_day_px: decimal_field(context, "prevDayPx"),
            day_ntl_vlm: decimal_field(context, "dayNtlVlm"),
            premium: None,
            oracle_px: None,
            mark_px: decimal_field(context, "markPx"),
            mid_px: decimal_field(context, "midPx"),
            impact_bid_px: None,
            impact_ask_px: None,
            max_leverage: None,
            sz_decimals: None,
            index: asset
                .get("index")
                .and_then(Value::as_i64)
                .or(Some(index as i64)),
            base,
            quote,
        });
    }
    Ok(rows)
}

#[derive(Debug, Deserialize)]
struct UniverseCsvRow {
    time_ns: i64,
    symbol: String,
    coin: String,
    security_type: String,
    market: String,
    universe: String,
    dex: String,
    source: String,
    is_historical: bool,
    value: String,
    funding: String,
    open_interest: String,
    prev_day_px: String,
    day_ntl_vlm: String,
    premium: String,
    oracle_px: String,
    mark_px: String,
    mid_px: String,
    impact_bid_px: String,
    impact_ask_px: String,
    max_leverage: String,
    sz_decimals: String,
    index: String,
    base: String,
    quote: String,
}

impl UniverseCsvRow {
    fn parse(line: &str) -> Option<Self> {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(line.as_bytes());
        reader.deserialize().next()?.ok()
    }
}

fn normalize_universe(value: &str) -> String {
    match value
        .trim()
        .replace(['-', '.', ':', ' '], "_")
        .to_ascii_uppercase()
        .as_str()
    {
        "PERP" | "PERPS" | "CRYPTOFUTURE" | "CRYPTO_FUTURE" | "CRYPTO_PERPS" => {
            "CRYPTO_PERP".to_string()
        }
        "SPOT" | "CRYPTO" => "CRYPTO_SPOT".to_string(),
        "HIP3_TRADING_XYZ" => "HIP3_XYZ".to_string(),
        other => other.to_string(),
    }
}

fn hip3_dex(universe: &str) -> Result<String> {
    let raw = universe
        .strip_prefix("HIP3_")
        .ok_or_else(|| anyhow::anyhow!("HIP-3 universe must start with HIP3_"))?;
    Ok(match raw {
        "TRADING_XYZ" => "xyz".to_string(),
        "XYZ" => "xyz".to_string(),
        "VNTL_XYZ" | "VNTL" => "vntl".to_string(),
        other => other.to_ascii_lowercase().replace('_', "."),
    })
}

fn parse_asset_ctx_time(raw: &str) -> Result<chrono::DateTime<Utc>> {
    if let Ok(ms) = raw.parse::<i64>() {
        return Utc
            .timestamp_millis_opt(ms)
            .single()
            .ok_or_else(|| anyhow::anyhow!("invalid asset_ctxs millisecond timestamp {raw}"));
    }
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(raw) {
        return Ok(dt.with_timezone(&Utc));
    }
    let dt = chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S")
        .with_context(|| format!("failed to parse asset_ctxs timestamp {raw}"))?;
    Ok(dt.and_utc())
}

fn should_emit_time(time: chrono::DateTime<Utc>, resolution: Resolution) -> bool {
    match resolution {
        Resolution::Minute => true,
        Resolution::Hour => time.minute() == 0,
        Resolution::Daily => time.hour() == 0 && time.minute() == 0,
        Resolution::Tick | Resolution::Second => false,
    }
}

fn parse_decimal(raw: &str) -> Option<Decimal> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    trimmed.parse().ok()
}

fn decimal_field(value: &Value, field: &str) -> Option<Decimal> {
    let value = value.get(field)?;
    if let Some(raw) = value.as_str() {
        return parse_decimal(raw);
    }
    value.as_f64().and_then(Decimal::from_f64_retain)
}

fn decimal_from_optional(raw: &str) -> Decimal {
    parse_decimal(raw).unwrap_or(Decimal::ZERO)
}

fn decimal_to_string(value: Option<Decimal>) -> String {
    value
        .map(|value| value.normalize().to_string())
        .unwrap_or_default()
}

fn option_i64_to_string(value: Option<i64>) -> String {
    value.map(|value| value.to_string()).unwrap_or_default()
}

fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn spot_base_quote(
    asset: &Value,
    token_by_index: &HashMap<i64, &Value>,
) -> (Option<String>, Option<String>) {
    let Some(tokens) = asset.get("tokens").and_then(Value::as_array) else {
        return (None, None);
    };
    let base = tokens
        .first()
        .and_then(Value::as_i64)
        .and_then(|index| token_by_index.get(&index))
        .and_then(|token| token.get("name"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase);
    let quote = tokens
        .get(1)
        .and_then(Value::as_i64)
        .and_then(|index| token_by_index.get(&index))
        .and_then(|token| token.get("name"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase);
    (base, quote)
}

fn config_string(
    config: &CustomDataConfig,
    plugin_config: &Map<String, Value>,
    key: &str,
) -> Option<String> {
    config
        .properties
        .get(key)
        .cloned()
        .or_else(|| {
            plugin_config
                .get(key)
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn config_path(
    config: &CustomDataConfig,
    plugin_config: &Map<String, Value>,
    key: &str,
) -> Option<String> {
    config_string(config, plugin_config, key)
}

fn archive_credentials(
    config: &CustomDataConfig,
    plugin_config: &Map<String, Value>,
) -> Option<ArchiveCredentials> {
    let access_key_id = config_string(config, plugin_config, "aws_access_key_id")
        .or_else(|| config_string(config, plugin_config, "AWS_ACCESS_KEY_ID"))
        .or_else(|| std::env::var("AWS_ACCESS_KEY_ID").ok())?;
    let secret_access_key = config_string(config, plugin_config, "aws_secret_access_key")
        .or_else(|| config_string(config, plugin_config, "AWS_SECRET_ACCESS_KEY"))
        .or_else(|| std::env::var("AWS_SECRET_ACCESS_KEY").ok())?;
    let session_token = config_string(config, plugin_config, "aws_session_token")
        .or_else(|| config_string(config, plugin_config, "AWS_SESSION_TOKEN"))
        .or_else(|| std::env::var("AWS_SESSION_TOKEN").ok());
    Some(ArchiveCredentials {
        access_key_id,
        secret_access_key,
        session_token,
    })
}

fn block_on_archive<F, T>(future: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to create Hyperliquid universe runtime")?
        .block_on(future)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn asset_ctx_universe_parser_filters_hip3_dex_and_hourly_rows() {
        let csv = "\
time,coin,funding,open_interest,prev_day_px,day_ntl_vlm,premium,oracle_px,mark_px,mid_px,impact_bid_px,impact_ask_px
2026-04-30 00:00:00,HYPE,0.0001,100,1,1000,0.01,1.1,1.2,1.3,1.29,1.31
2026-04-30 00:00:00,xyz:TSLA,-0.0002,200,2,2000,-0.01,2.1,2.2,2.3,2.29,2.31
2026-04-30 00:01:00,xyz:NVDA,-0.0003,300,3,3000,-0.02,3.1,3.2,3.3,3.29,3.31
";
        let rows = parse_asset_ctx_universe_rows(
            csv,
            "HIP3_XYZ",
            Some("xyz"),
            NaiveDate::from_ymd_opt(2026, 4, 30).unwrap(),
            Resolution::Hour,
        )
        .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].symbol, "XYZ:TSLA");
        assert_eq!(rows[0].dex, "xyz");
        assert_eq!(rows[0].source, "asset_ctxs");
        assert!(rows[0].is_historical);
        assert_eq!(rows[0].funding, Some(Decimal::new(-2, 4)));
    }

    #[test]
    fn hip3_universe_names_map_to_api_dex_ids() {
        assert_eq!(hip3_dex("HIP3_XYZ").unwrap(), "xyz");
        assert_eq!(hip3_dex("HIP3_TRADING_XYZ").unwrap(), "xyz");
        assert_eq!(hip3_dex("HIP3_VNTL").unwrap(), "vntl");
    }

    #[test]
    fn meta_api_universe_rows_preserve_source_coin_casing() {
        let response = json!([
            {
                "universe": [
                    { "name": "xyz:TSLA", "maxLeverage": 3, "szDecimals": 2 }
                ]
            },
            [
                {
                    "funding": "-0.0001",
                    "openInterest": "1000",
                    "markPx": "250.5",
                    "midPx": "250.6"
                }
            ]
        ]);

        let rows = parse_meta_and_asset_ctx_rows(
            &response,
            "HIP3_XYZ",
            "xyz",
            NaiveDate::from_ymd_opt(2026, 4, 30).unwrap(),
        )
        .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].symbol, "XYZ:TSLA");
        assert_eq!(rows[0].coin, "xyz:TSLA");
        assert_eq!(rows[0].source, "info_api_current");
        assert!(!rows[0].is_historical);
    }

    #[test]
    fn spot_meta_api_universe_rows_create_crypto_symbols() {
        let response = json!([
            {
                "tokens": [
                    { "index": 0, "name": "PURR" },
                    { "index": 1, "name": "USDC" }
                ],
                "universe": [
                    { "name": "PURR/USDC", "index": 0, "tokens": [0, 1] }
                ]
            },
            [
                {
                    "prevDayPx": "0.1",
                    "dayNtlVlm": "100000",
                    "markPx": "0.11",
                    "midPx": "0.12"
                }
            ]
        ]);

        let rows = parse_spot_meta_and_asset_ctx_rows(
            &response,
            "CRYPTO_SPOT",
            NaiveDate::from_ymd_opt(2026, 4, 30).unwrap(),
        )
        .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].symbol, "PURR/USDC");
        assert_eq!(rows[0].security_type, SecurityType::Crypto);
        assert_eq!(rows[0].base.as_deref(), Some("PURR"));
        assert_eq!(rows[0].quote.as_deref(), Some("USDC"));
        assert_eq!(rows[0].source, "info_api_current");
    }
}
