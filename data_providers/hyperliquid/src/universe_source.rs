use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::{NaiveDate, TimeZone, Timelike, Utc};
use lean_core::{DateTime, Market, NanosecondTimestamp, Resolution, SecurityType, TimeSpan};
use lean_data::custom::{CustomDataConfig, CustomDataFormat, CustomDataPoint, CustomDataSource};
use lean_data_providers::{CustomDataContext, ICustomDataSource};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{json, Map, Value};

use crate::archive::{ArchiveBuckets, ArchiveCredentials, ArchiveRegions, S3ArchiveClient};
use crate::history_provider::{
    archive_coin_key, asset_contexts_key, fills_key, parse_archive_records, parse_timestamp,
    HyperliquidInfoClient,
};

const DEFAULT_INFO_URL: &str = "https://api.hyperliquid.xyz/info";
const HEADER: &str = "time_ns,symbol,coin,security_type,market,universe,dex,source,is_historical,value,funding,open_interest,prev_day_px,day_ntl_vlm,premium,oracle_px,mark_px,mid_px,impact_bid_px,impact_ask_px,max_leverage,sz_decimals,index,base,quote";
const FUNDING_CACHE_HEADER: &str = "time_ns,funding";
const CANDLE_CACHE_HEADER: &str = "time_ns,open,high,low,close,volume";
const MAX_FUNDING_ROWS_PER_REQUEST: usize = 500;
const MAX_CANDLES_PER_REQUEST: i64 = 5_000;

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

    fn funding_cache_path(&self, dex: &str, coin: &str) -> PathBuf {
        self.data_root
            .join("custom")
            .join("hyperliquid")
            .join("funding_history")
            .join(dex.to_ascii_lowercase())
            .join(format!("{}.csv", sanitize_cache_component(coin)))
    }

    fn candle_cache_path(&self, dex: &str, coin: &str) -> PathBuf {
        self.data_root
            .join("custom")
            .join("hyperliquid")
            .join("candle_history")
            .join(dex.to_ascii_lowercase())
            .join(format!("{}.csv", sanitize_cache_component(coin)))
    }

    fn ensure_raw_file(
        &self,
        ticker: &str,
        date: NaiveDate,
        config: &CustomDataConfig,
    ) -> Result<Option<PathBuf>> {
        let path = self.raw_path(ticker, date);
        if path.exists() && !raw_file_needs_impact_backfill(&path, ticker, config) {
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
                let historical =
                    self.load_asset_ctx_rows(universe, Some(&dex), date, resolution, config)?;
                if historical.as_ref().is_some_and(|rows| !rows.is_empty()) {
                    return Ok(historical);
                }
                let rows = if matches!(resolution, Resolution::Hour | Resolution::Daily) {
                    self.load_hip3_info_history_rows(universe, &dex, date, resolution, config)?
                } else {
                    self.load_hip3_fills_rows(universe, &dex, date, resolution, config)?
                };
                Ok((!rows.is_empty()).then_some(rows))
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
        let Some(text) = block_on_archive(archive.market_text(&key))? else {
            return Ok(None);
        };
        let info = self.info_client(config);
        let metadata = load_current_perp_metadata(&info, dex_filter)?;
        let rows = parse_asset_ctx_universe_rows(
            &text, universe, dex_filter, date, resolution, &metadata,
        )?;
        Ok(Some(rows))
    }

    fn load_hip3_info_history_rows(
        &self,
        universe: &str,
        dex: &str,
        date: NaiveDate,
        resolution: Resolution,
        config: &CustomDataConfig,
    ) -> Result<Vec<UniverseRow>> {
        if !matches!(resolution, Resolution::Hour | Resolution::Daily) {
            return Err(anyhow::anyhow!(
                "Hyperliquid HIP-3 REST universe history supports Hour and Daily resolutions"
            ));
        }

        let info = self.info_client(config);
        let metadata = load_current_perp_metadata(&info, Some(dex))?;
        let mut coins = metadata
            .values()
            .map(|meta| meta.coin.clone())
            .collect::<Vec<_>>();
        coins.sort_by_key(|coin| archive_coin_key(coin));
        let mut rows = Vec::new();
        for coin in coins {
            let metadata = metadata
                .get(&archive_coin_key(&coin))
                .with_context(|| format!("missing Hyperliquid metadata for {coin}"))?;
            let candles = self.candle_rows_for_coin(&info, dex, &coin, date)?;
            let day_candles = candles
                .into_iter()
                .filter(|candle| candle.time.date_utc() == date)
                .collect::<Vec<_>>();
            if day_candles.is_empty() {
                continue;
            }
            let funding_rows = self.funding_rows_for_coin(&info, dex, &coin, date)?;

            match resolution {
                Resolution::Hour => {
                    let funding_by_time = funding_rows_by_bucket(funding_rows, Resolution::Hour)?;
                    rows.extend(day_candles.into_iter().map(|candle| {
                        universe_row_from_candle(
                            universe,
                            dex,
                            &coin,
                            &candle,
                            funding_by_time.get(&candle.time.0).copied(),
                            metadata,
                        )
                    }));
                }
                Resolution::Daily => {
                    if let Some(candle) = aggregate_candles_for_day(&day_candles, date) {
                        let funding_by_time =
                            funding_rows_by_bucket(funding_rows, Resolution::Daily)?;
                        let funding_time = date_to_datetime(date, 0, 0, 0);
                        let funding = funding_by_time.get(&funding_time.0).copied();
                        rows.push(universe_row_from_candle(
                            universe, dex, &coin, &candle, funding, metadata,
                        ));
                    }
                }
                Resolution::Minute | Resolution::Tick | Resolution::Second => unreachable!(),
            }
        }
        rows.sort_by(|left, right| {
            (left.time_ns, left.symbol.as_str()).cmp(&(right.time_ns, right.symbol.as_str()))
        });
        Ok(rows)
    }

    fn candle_rows_for_coin(
        &self,
        info: &HyperliquidInfoClient,
        dex: &str,
        coin: &str,
        date: NaiveDate,
    ) -> Result<Vec<CandleCacheRow>> {
        let cache_path = self.candle_cache_path(dex, coin);
        let cached = read_candle_cache(&cache_path)?;
        if candle_cache_covers_date(&cached, date) {
            return Ok(cached);
        }

        let start = date_to_datetime(date, 0, 0, 0);
        let end = DateTime::from(Utc::now());
        let mut rows_by_time = BTreeMap::new();
        for row in cached {
            rows_by_time.insert(row.time.0, row);
        }
        let mut current = start.as_millis();
        let end_ms = end.as_millis();

        while current <= end_ms {
            let chunk_end = end_ms.min(current + 3_600_000 * MAX_CANDLES_PER_REQUEST - 1);
            let response = info.candle_snapshot(coin, "1h", current, chunk_end)?;
            let rows = parse_candle_history_rows(&response, start, end)?;
            let last_ms = rows.iter().map(|row| row.time.as_millis()).max();
            for row in rows {
                rows_by_time.insert(row.time.0, row);
            }

            current = match last_ms {
                Some(last) if last >= current => last + 3_600_000,
                _ => chunk_end + 1,
            };
        }

        let rows = rows_by_time.into_values().collect::<Vec<_>>();
        write_candle_cache(&cache_path, &rows)?;
        Ok(rows)
    }

    fn load_hip3_fills_rows(
        &self,
        universe: &str,
        dex: &str,
        date: NaiveDate,
        resolution: Resolution,
        config: &CustomDataConfig,
    ) -> Result<Vec<UniverseRow>> {
        if matches!(resolution, Resolution::Tick | Resolution::Second) {
            return Err(anyhow::anyhow!(
                "Hyperliquid HIP-3 universe rows support Minute, Hour, and Daily resolutions"
            ));
        }

        let archive = self.archive_client(config);
        let mut aggregates: BTreeMap<(String, i64), Hip3FillAggregate> = BTreeMap::new();
        for hour in 0..24 {
            let key = fills_key(date_hour(date, hour));
            let Some(text) = block_on_archive(archive.fills_text(&key))? else {
                continue;
            };
            parse_hip3_fill_aggregates(&text, dex, date, resolution, &mut aggregates)
                .with_context(|| format!("failed to parse Hyperliquid fills {key}"))?;
        }

        if aggregates.is_empty() {
            return Ok(Vec::new());
        }

        let mut daily_notional_by_coin: HashMap<String, Decimal> = HashMap::new();
        let mut coins = HashSet::new();
        for aggregate in aggregates.values() {
            coins.insert(aggregate.coin.clone());
            *daily_notional_by_coin
                .entry(aggregate.coin_key.clone())
                .or_insert(Decimal::ZERO) += aggregate.notional;
        }
        let info = self.info_client(config);
        let metadata = load_current_perp_metadata(&info, Some(dex))?;
        let funding_by_coin_time =
            self.load_hip3_funding_rows(dex, &coins, date, resolution, config)?;

        let mut rows = Vec::with_capacity(aggregates.len());
        for aggregate in aggregates.values() {
            let funding_time =
                funding_lookup_bucket_time(NanosecondTimestamp(aggregate.time_ns), resolution)?;
            let funding = funding_by_coin_time
                .get(&(aggregate.coin_key.clone(), funding_time.0))
                .copied();
            let metadata = metadata
                .get(&aggregate.coin_key)
                .with_context(|| format!("missing Hyperliquid metadata for {}", aggregate.coin))?;
            rows.push(UniverseRow {
                time_ns: aggregate.time_ns,
                symbol: aggregate.coin.to_ascii_uppercase(),
                coin: aggregate.coin.clone(),
                security_type: SecurityType::CryptoFuture,
                market: Market::HYPERLIQUID.to_string(),
                universe: universe.to_string(),
                dex: dex.to_string(),
                source: "node_fills_by_block".to_string(),
                is_historical: true,
                value: Some(aggregate.close),
                funding,
                open_interest: None,
                prev_day_px: None,
                day_ntl_vlm: daily_notional_by_coin.get(&aggregate.coin_key).copied(),
                premium: None,
                oracle_px: None,
                mark_px: Some(aggregate.close),
                mid_px: Some(aggregate.close),
                impact_bid_px: None,
                impact_ask_px: None,
                max_leverage: Some(metadata.max_leverage),
                sz_decimals: metadata.sz_decimals,
                index: metadata.index,
                base: aggregate
                    .coin
                    .split_once(':')
                    .map(|(_, base)| base.to_ascii_uppercase()),
                quote: Some("USDC".to_string()),
            });
        }
        rows.sort_by(|left, right| {
            (left.time_ns, left.symbol.as_str()).cmp(&(right.time_ns, right.symbol.as_str()))
        });
        Ok(rows)
    }

    fn load_hip3_funding_rows(
        &self,
        dex: &str,
        coins: &HashSet<String>,
        date: NaiveDate,
        resolution: Resolution,
        config: &CustomDataConfig,
    ) -> Result<HashMap<(String, i64), Decimal>> {
        let info = self.info_client(config);
        let mut funding_by_coin_time = HashMap::new();
        let funding_resolution = funding_bucket_resolution(resolution)?;

        let mut sorted_coins = coins.iter().collect::<Vec<_>>();
        sorted_coins.sort();
        for coin in sorted_coins {
            let rows = self.funding_rows_for_coin(&info, dex, coin, date)?;
            for (time_ns, funding) in funding_rows_by_bucket(rows, funding_resolution)? {
                if NanosecondTimestamp(time_ns).date_utc() == date {
                    funding_by_coin_time.insert((archive_coin_key(coin), time_ns), funding);
                }
            }
        }
        Ok(funding_by_coin_time)
    }

    fn funding_rows_for_coin(
        &self,
        info: &HyperliquidInfoClient,
        dex: &str,
        coin: &str,
        date: NaiveDate,
    ) -> Result<Vec<(DateTime, Decimal)>> {
        let cache_path = self.funding_cache_path(dex, coin);
        let cached = read_funding_cache(&cache_path)?;
        if funding_cache_covers_date(&cached, date) {
            return Ok(cached);
        }

        let start = date_to_datetime(date, 0, 0, 0);
        let end = DateTime::from(Utc::now());
        let mut rows_by_time = BTreeMap::new();
        for (time, funding) in cached {
            rows_by_time.insert(time.0, funding);
        }

        let mut current = start.as_millis();
        let end_ms = end.as_millis();
        while current <= end_ms {
            let response = info.funding_history(coin, current, end_ms)?;
            let rows = parse_funding_history_rows(&response, start, end)?;
            let last_ms = rows.iter().map(|(time, _)| time.as_millis()).max();
            let count = rows.len();
            for (time, funding) in rows {
                rows_by_time.insert(time.0, funding);
            }

            current = match last_ms {
                Some(last) if last >= current => last + 1,
                _ => break,
            };
            if count < MAX_FUNDING_ROWS_PER_REQUEST {
                break;
            }
        }

        let rows = rows_by_time
            .into_iter()
            .map(|(time_ns, funding)| (NanosecondTimestamp(time_ns), funding))
            .collect::<Vec<_>>();
        write_funding_cache(&cache_path, &rows)?;
        Ok(rows)
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

fn raw_file_needs_impact_backfill(path: &PathBuf, ticker: &str, config: &CustomDataConfig) -> bool {
    let universe = normalize_universe(
        config
            .properties
            .get("universe")
            .map(String::as_str)
            .unwrap_or(ticker),
    );
    if !universe.starts_with("HIP3_") {
        return false;
    }

    let Ok(text) = std::fs::read_to_string(path) else {
        return false;
    };
    text.lines().skip(1).any(|line| {
        let Some(row) = UniverseCsvRow::parse(line) else {
            return false;
        };
        row.source == "info_api_candle"
            && (row.impact_bid_px.trim().is_empty() || row.impact_ask_px.trim().is_empty())
    })
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
                headers: std::collections::HashMap::new(),
                // Universe rows carry a per-row underlying ticker in the "symbol"
                // column. (Only consulted by the engine's Parquet decode path;
                // this CSV source sets `CustomDataPoint::symbol` in `reader`.)
                symbol_column: Some("symbol".to_string()),
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

        Some(
            CustomDataPoint::new(date, Some(time), decimal_from_optional(&row.value), fields)
                .with_symbol(Some(row.symbol.clone())),
        )
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
    metadata: &HashMap<String, PerpMetadata>,
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
        let metadata = metadata
            .get(&archive_coin_key(&source_coin))
            .with_context(|| format!("missing Hyperliquid metadata for {source_coin}"))?;
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
            max_leverage: Some(metadata.max_leverage),
            sz_decimals: metadata.sz_decimals,
            index: metadata.index,
            base: None,
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

#[derive(Debug, Clone, PartialEq)]
struct CandleCacheRow {
    time: DateTime,
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    volume: Decimal,
}

#[derive(Debug, Deserialize)]
struct CandleCacheCsvRow {
    time_ns: i64,
    open: String,
    high: String,
    low: String,
    close: String,
    volume: String,
}

#[derive(Debug, Clone)]
struct PerpMetadata {
    coin: String,
    max_leverage: i64,
    sz_decimals: Option<i64>,
    index: Option<i64>,
    impact_bid_ratio: Option<Decimal>,
    impact_ask_ratio: Option<Decimal>,
}

fn load_current_perp_metadata(
    info: &HyperliquidInfoClient,
    dex: Option<&str>,
) -> Result<HashMap<String, PerpMetadata>> {
    let response = info.meta_and_asset_ctxs(dex)?;
    let array = response
        .as_array()
        .filter(|array| !array.is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!("Hyperliquid metaAndAssetCtxs response must include meta")
        })?;
    let universe_rows = array[0]
        .get("universe")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            anyhow::anyhow!("Hyperliquid metaAndAssetCtxs response missing meta.universe")
        })?;
    let contexts = array.get(1).and_then(Value::as_array);
    let mut metadata = HashMap::new();
    for (index, asset) in universe_rows.iter().enumerate() {
        let Some(name) = asset.get("name").and_then(Value::as_str).map(str::trim) else {
            continue;
        };
        if name.is_empty() {
            continue;
        }
        let max_leverage = asset
            .get("maxLeverage")
            .and_then(Value::as_i64)
            .or_else(|| asset.get("max_leverage").and_then(Value::as_i64))
            .with_context(|| format!("Hyperliquid metadata for {name} missing maxLeverage"))?;
        let (impact_bid_ratio, impact_ask_ratio) = contexts
            .and_then(|contexts| contexts.get(index))
            .and_then(impact_quote_ratios_from_context)
            .map(|(bid, ask)| (Some(bid), Some(ask)))
            .unwrap_or((None, None));
        metadata.insert(
            archive_coin_key(name),
            PerpMetadata {
                coin: name.to_string(),
                max_leverage,
                sz_decimals: asset.get("szDecimals").and_then(Value::as_i64),
                index: asset
                    .get("index")
                    .and_then(Value::as_i64)
                    .or(Some(index as i64)),
                impact_bid_ratio,
                impact_ask_ratio,
            },
        );
    }
    Ok(metadata)
}

fn impact_quote_ratios_from_context(context: &Value) -> Option<(Decimal, Decimal)> {
    let mid = decimal_field(context, "midPx").or_else(|| decimal_field(context, "markPx"))?;
    let impact_pxs = context.get("impactPxs").and_then(Value::as_array)?;
    let first = impact_pxs.first().and_then(decimal_value)?;
    let second = impact_pxs.get(1).and_then(decimal_value)?;
    if mid <= Decimal::ZERO || first <= Decimal::ZERO || second <= Decimal::ZERO {
        return None;
    }

    let bid = first.min(second);
    let ask = first.max(second);
    if ask < bid {
        return None;
    }
    Some((bid / mid, ask / mid))
}

fn parse_candle_history_rows(
    response: &Value,
    start: DateTime,
    end: DateTime,
) -> Result<Vec<CandleCacheRow>> {
    let rows = response
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("Hyperliquid candleSnapshot response must be an array"))?;
    let mut parsed = Vec::new();
    for row in rows {
        let Some(time) = row.get("t").and_then(parse_timestamp) else {
            continue;
        };
        if time < start || time > end {
            continue;
        }
        let Some(open) = decimal_value_field(row, "o") else {
            continue;
        };
        let Some(high) = decimal_value_field(row, "h") else {
            continue;
        };
        let Some(low) = decimal_value_field(row, "l") else {
            continue;
        };
        let Some(close) = decimal_value_field(row, "c") else {
            continue;
        };
        let Some(volume) = decimal_value_field(row, "v") else {
            continue;
        };
        if open <= Decimal::ZERO
            || high <= Decimal::ZERO
            || low <= Decimal::ZERO
            || close <= Decimal::ZERO
        {
            continue;
        }
        parsed.push(CandleCacheRow {
            time,
            open,
            high,
            low,
            close,
            volume,
        });
    }
    parsed.sort_by_key(|row| row.time.0);
    parsed.dedup_by_key(|row| row.time.0);
    Ok(parsed)
}

fn aggregate_candles_for_day(
    candles: &[CandleCacheRow],
    date: NaiveDate,
) -> Option<CandleCacheRow> {
    let first = candles.first()?;
    let last = candles.last()?;
    let high = candles
        .iter()
        .map(|candle| candle.high)
        .max()
        .unwrap_or(first.high);
    let low = candles
        .iter()
        .map(|candle| candle.low)
        .min()
        .unwrap_or(first.low);
    let volume = candles
        .iter()
        .map(|candle| candle.volume)
        .fold(Decimal::ZERO, |sum, value| sum + value);
    Some(CandleCacheRow {
        time: date_to_datetime(date, 0, 0, 0),
        open: first.open,
        high,
        low,
        close: last.close,
        volume,
    })
}

fn universe_row_from_candle(
    universe: &str,
    dex: &str,
    coin: &str,
    candle: &CandleCacheRow,
    funding: Option<Decimal>,
    metadata: &PerpMetadata,
) -> UniverseRow {
    let notional = candle.close * candle.volume;
    UniverseRow {
        time_ns: candle.time.0,
        symbol: coin.to_ascii_uppercase(),
        coin: coin.to_string(),
        security_type: SecurityType::CryptoFuture,
        market: Market::HYPERLIQUID.to_string(),
        universe: universe.to_string(),
        dex: dex.to_string(),
        source: "info_api_candle".to_string(),
        is_historical: true,
        value: Some(candle.close),
        funding,
        open_interest: None,
        prev_day_px: None,
        day_ntl_vlm: Some(notional),
        premium: None,
        oracle_px: None,
        mark_px: Some(candle.close),
        mid_px: Some(candle.close),
        impact_bid_px: metadata.impact_bid_ratio.map(|ratio| candle.close * ratio),
        impact_ask_px: metadata.impact_ask_ratio.map(|ratio| candle.close * ratio),
        max_leverage: Some(metadata.max_leverage),
        sz_decimals: metadata.sz_decimals,
        index: metadata.index,
        base: coin
            .split_once(':')
            .map(|(_, base)| base.to_ascii_uppercase()),
        quote: Some("USDC".to_string()),
    }
}

#[derive(Debug, Clone)]
struct Hip3FillAggregate {
    coin_key: String,
    coin: String,
    time_ns: i64,
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    volume: Decimal,
    notional: Decimal,
    first_trade_ns: i64,
    last_trade_ns: i64,
}

impl Hip3FillAggregate {
    fn new(
        coin: &str,
        bucket_time: DateTime,
        trade_time: DateTime,
        price: Decimal,
        size: Decimal,
    ) -> Self {
        Self {
            coin_key: archive_coin_key(coin),
            coin: coin.to_string(),
            time_ns: bucket_time.0,
            open: price,
            high: price,
            low: price,
            close: price,
            volume: size,
            notional: price * size,
            first_trade_ns: trade_time.0,
            last_trade_ns: trade_time.0,
        }
    }

    fn update(&mut self, trade_time: DateTime, price: Decimal, size: Decimal) {
        if trade_time.0 < self.first_trade_ns {
            self.first_trade_ns = trade_time.0;
            self.open = price;
        }
        if trade_time.0 >= self.last_trade_ns {
            self.last_trade_ns = trade_time.0;
            self.close = price;
        }
        if price > self.high {
            self.high = price;
        }
        if price < self.low {
            self.low = price;
        }
        self.volume += size;
        self.notional += price * size;
    }
}

fn parse_hip3_fill_aggregates(
    text: &str,
    dex: &str,
    date: NaiveDate,
    resolution: Resolution,
    aggregates: &mut BTreeMap<(String, i64), Hip3FillAggregate>,
) -> Result<()> {
    let dex_prefix = format!("{}:", dex.to_ascii_lowercase());
    for record in parse_archive_records(text)? {
        let Some(events) = record.get("events").and_then(Value::as_array) else {
            continue;
        };
        for event in events {
            let fill = if let Some(array) = event.as_array() {
                array.get(1).unwrap_or(event)
            } else {
                event
            };
            let Some(coin) = fill.get("coin").and_then(Value::as_str) else {
                continue;
            };
            if !coin.to_ascii_lowercase().starts_with(&dex_prefix) {
                continue;
            }
            let Some(price) = decimal_value_field(fill, "px") else {
                continue;
            };
            let Some(size) = decimal_value_field(fill, "sz") else {
                continue;
            };
            if price <= Decimal::ZERO || size <= Decimal::ZERO {
                continue;
            }
            let Some(trade_time) = fill
                .get("time")
                .and_then(parse_timestamp)
                .or_else(|| record.get("block_time").and_then(parse_timestamp))
            else {
                continue;
            };
            if trade_time.date_utc() != date {
                continue;
            }
            let bucket_time = universe_bucket_time(trade_time, resolution)?;
            let coin_key = archive_coin_key(coin);
            let key = (coin_key, bucket_time.0);
            match aggregates.get_mut(&key) {
                Some(aggregate) => aggregate.update(trade_time, price, size),
                None => {
                    aggregates.insert(
                        key,
                        Hip3FillAggregate::new(coin, bucket_time, trade_time, price, size),
                    );
                }
            }
        }
    }
    Ok(())
}

fn parse_funding_history_rows(
    response: &Value,
    start: DateTime,
    end: DateTime,
) -> Result<Vec<(DateTime, Decimal)>> {
    let rows = response
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("Hyperliquid fundingHistory response must be an array"))?;
    let mut parsed = Vec::new();
    for row in rows {
        let Some(time) = row.get("time").and_then(parse_timestamp) else {
            continue;
        };
        if time < start || time > end {
            continue;
        }
        let Some(funding) =
            decimal_value_field(row, "fundingRate").or_else(|| decimal_value_field(row, "funding"))
        else {
            continue;
        };
        parsed.push((time, funding));
    }
    parsed.sort_by_key(|(time, _)| time.0);
    parsed.dedup_by_key(|(time, _)| time.0);
    Ok(parsed)
}

fn funding_bucket_resolution(resolution: Resolution) -> Result<Resolution> {
    match resolution {
        Resolution::Minute | Resolution::Hour => Ok(Resolution::Hour),
        Resolution::Daily => Ok(Resolution::Daily),
        Resolution::Tick | Resolution::Second => Err(anyhow::anyhow!(
            "Hyperliquid funding history cannot be bucketed for {resolution:?} universe rows"
        )),
    }
}

fn funding_lookup_bucket_time(time: DateTime, resolution: Resolution) -> Result<DateTime> {
    universe_bucket_time(time, funding_bucket_resolution(resolution)?)
}

fn funding_rows_by_bucket(
    rows: Vec<(DateTime, Decimal)>,
    resolution: Resolution,
) -> Result<HashMap<i64, Decimal>> {
    let mut buckets: BTreeMap<i64, (Decimal, u64)> = BTreeMap::new();
    for (time, funding) in rows {
        let bucket_time = universe_bucket_time(time, resolution)?;
        let entry = buckets.entry(bucket_time.0).or_insert((Decimal::ZERO, 0));
        entry.0 += funding;
        entry.1 += 1;
    }

    Ok(buckets
        .into_iter()
        .map(|(time_ns, (sum, count))| (time_ns, sum / Decimal::from(count)))
        .collect())
}

fn decimal_value_field(value: &Value, field: &str) -> Option<Decimal> {
    let value = value.get(field)?;
    if let Some(raw) = value.as_str() {
        return parse_decimal(raw);
    }
    value.as_f64().and_then(Decimal::from_f64_retain)
}

fn universe_bucket_time(time: DateTime, resolution: Resolution) -> Result<DateTime> {
    match resolution {
        Resolution::Minute => Ok(NanosecondTimestamp(
            time.0.div_euclid(TimeSpan::ONE_MINUTE.nanos) * TimeSpan::ONE_MINUTE.nanos,
        )),
        Resolution::Hour => Ok(NanosecondTimestamp(
            time.0.div_euclid(TimeSpan::ONE_HOUR.nanos) * TimeSpan::ONE_HOUR.nanos,
        )),
        Resolution::Daily => Ok(date_to_datetime(time.date_utc(), 0, 0, 0)),
        Resolution::Tick | Resolution::Second => Err(anyhow::anyhow!(
            "Hyperliquid HIP-3 universe rows do not support {resolution:?}"
        )),
    }
}

fn date_to_datetime(date: NaiveDate, hour: u32, minute: u32, second: u32) -> DateTime {
    DateTime::from(
        date.and_hms_opt(hour, minute, second)
            .expect("valid UTC wall-clock time")
            .and_utc(),
    )
}

fn date_hour(date: NaiveDate, hour: u32) -> chrono::DateTime<Utc> {
    date.and_hms_opt(hour, 0, 0)
        .expect("valid UTC hour")
        .and_utc()
}

#[derive(Debug, Deserialize)]
struct FundingCacheCsvRow {
    time_ns: i64,
    funding: String,
}

fn read_funding_cache(path: &PathBuf) -> Result<Vec<(DateTime, Decimal)>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let mut rows = Vec::new();
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(text.as_bytes());
    for row in reader.deserialize::<FundingCacheCsvRow>() {
        let row = row.with_context(|| {
            format!(
                "failed to parse Hyperliquid funding cache row in {}",
                path.display()
            )
        })?;
        if let Some(funding) = parse_decimal(&row.funding) {
            rows.push((NanosecondTimestamp(row.time_ns), funding));
        }
    }
    rows.sort_by_key(|(time, _)| time.0);
    rows.dedup_by_key(|(time, _)| time.0);
    Ok(rows)
}

fn write_funding_cache(path: &PathBuf, rows: &[(DateTime, Decimal)]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let mut text = String::with_capacity(rows.len() * 32);
    text.push_str(FUNDING_CACHE_HEADER);
    text.push('\n');
    for (time, funding) in rows {
        text.push_str(&time.0.to_string());
        text.push(',');
        text.push_str(&funding.normalize().to_string());
        text.push('\n');
    }
    std::fs::write(path, text).with_context(|| format!("failed to write {}", path.display()))
}

fn read_candle_cache(path: &PathBuf) -> Result<Vec<CandleCacheRow>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let mut rows = Vec::new();
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(text.as_bytes());
    for row in reader.deserialize::<CandleCacheCsvRow>() {
        let row = row.with_context(|| {
            format!(
                "failed to parse Hyperliquid candle cache row in {}",
                path.display()
            )
        })?;
        let Some(open) = parse_decimal(&row.open) else {
            continue;
        };
        let Some(high) = parse_decimal(&row.high) else {
            continue;
        };
        let Some(low) = parse_decimal(&row.low) else {
            continue;
        };
        let Some(close) = parse_decimal(&row.close) else {
            continue;
        };
        let Some(volume) = parse_decimal(&row.volume) else {
            continue;
        };
        rows.push(CandleCacheRow {
            time: NanosecondTimestamp(row.time_ns),
            open,
            high,
            low,
            close,
            volume,
        });
    }
    rows.sort_by_key(|row| row.time.0);
    rows.dedup_by_key(|row| row.time.0);
    Ok(rows)
}

fn write_candle_cache(path: &PathBuf, rows: &[CandleCacheRow]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let mut text = String::with_capacity(rows.len() * 80);
    text.push_str(CANDLE_CACHE_HEADER);
    text.push('\n');
    for row in rows {
        text.push_str(&row.time.0.to_string());
        text.push(',');
        text.push_str(&row.open.normalize().to_string());
        text.push(',');
        text.push_str(&row.high.normalize().to_string());
        text.push(',');
        text.push_str(&row.low.normalize().to_string());
        text.push(',');
        text.push_str(&row.close.normalize().to_string());
        text.push(',');
        text.push_str(&row.volume.normalize().to_string());
        text.push('\n');
    }
    std::fs::write(path, text).with_context(|| format!("failed to write {}", path.display()))
}

fn funding_cache_covers_date(rows: &[(DateTime, Decimal)], date: NaiveDate) -> bool {
    if rows.iter().any(|(time, _)| time.date_utc() == date) {
        return true;
    }
    let Some(first) = rows.first().map(|(time, _)| *time) else {
        return false;
    };
    let Some(last) = rows.last().map(|(time, _)| *time) else {
        return false;
    };
    first <= date_to_datetime(date, 0, 0, 0) && last >= date_to_datetime(date, 23, 0, 0)
}

fn candle_cache_covers_date(rows: &[CandleCacheRow], date: NaiveDate) -> bool {
    if rows.iter().any(|row| row.time.date_utc() == date) {
        return true;
    }
    let Some(first) = rows.first().map(|row| row.time) else {
        return false;
    };
    let Some(last) = rows.last().map(|row| row.time) else {
        return false;
    };
    first <= date_to_datetime(date, 0, 0, 0) && last >= date_to_datetime(date, 23, 0, 0)
}

fn sanitize_cache_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect()
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
    decimal_value(value)
}

fn decimal_value(value: &Value) -> Option<Decimal> {
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
        let metadata = HashMap::from([(
            archive_coin_key("xyz:TSLA"),
            PerpMetadata {
                coin: "xyz:TSLA".to_string(),
                max_leverage: 3,
                sz_decimals: Some(2),
                index: Some(7),
                impact_bid_ratio: None,
                impact_ask_ratio: None,
            },
        )]);
        let rows = parse_asset_ctx_universe_rows(
            csv,
            "HIP3_XYZ",
            Some("xyz"),
            NaiveDate::from_ymd_opt(2026, 4, 30).unwrap(),
            Resolution::Hour,
            &metadata,
        )
        .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].symbol, "XYZ:TSLA");
        assert_eq!(rows[0].dex, "xyz");
        assert_eq!(rows[0].source, "asset_ctxs");
        assert!(rows[0].is_historical);
        assert_eq!(rows[0].funding, Some(Decimal::new(-2, 4)));
        assert_eq!(rows[0].max_leverage, Some(3));
        assert_eq!(rows[0].sz_decimals, Some(2));
        assert_eq!(rows[0].index, Some(7));
    }

    #[test]
    fn hip3_universe_names_map_to_api_dex_ids() {
        assert_eq!(hip3_dex("HIP3_XYZ").unwrap(), "xyz");
        assert_eq!(hip3_dex("HIP3_TRADING_XYZ").unwrap(), "xyz");
        assert_eq!(hip3_dex("HIP3_VNTL").unwrap(), "vntl");
    }

    #[test]
    fn hip3_fill_aggregates_build_historical_hourly_rows() {
        let text = r#"{"block_time":"2026-04-30T00:00:00.000000000","events":[["0x1",{"coin":"xyz:TSLA","px":"250.0","sz":"2","time":1777507201000,"tid":"a"}],["0x2",{"coin":"xyz:TSLA","px":"255.0","sz":"3","time":1777509000000,"tid":"b"}],["0x3",{"coin":"HYPE","px":"30","sz":"1","time":1777509000000,"tid":"c"}]]}
{"block_time":"2026-04-30T01:00:00.000000000","events":[["0x4",{"coin":"xyz:NVDA","px":"120.0","sz":"4","time":1777510800000,"tid":"d"}]]}
"#;
        let mut aggregates = BTreeMap::new();
        parse_hip3_fill_aggregates(
            text,
            "xyz",
            NaiveDate::from_ymd_opt(2026, 4, 30).unwrap(),
            Resolution::Hour,
            &mut aggregates,
        )
        .unwrap();

        assert_eq!(aggregates.len(), 2);
        let tsla = aggregates
            .get(&("XYZ:TSLA".to_string(), 1_777_507_200_000_000_000))
            .unwrap();
        assert_eq!(tsla.coin, "xyz:TSLA");
        assert_eq!(tsla.open, Decimal::new(2500, 1));
        assert_eq!(tsla.close, Decimal::new(2550, 1));
        assert_eq!(tsla.volume, Decimal::new(5, 0));
        assert_eq!(tsla.notional, Decimal::new(12650, 1));
    }

    #[test]
    fn parses_hip3_funding_history_rows() {
        let response = json!([
            { "coin": "xyz:TSLA", "fundingRate": "-0.0000125", "premium": "0.0", "time": 1777507200000_i64 },
            { "coin": "xyz:TSLA", "fundingRate": "0.000003", "premium": "0.0", "time": 1777510800000_i64 }
        ]);

        let rows = parse_funding_history_rows(
            &response,
            date_to_datetime(NaiveDate::from_ymd_opt(2026, 4, 30).unwrap(), 0, 0, 0),
            date_to_datetime(NaiveDate::from_ymd_opt(2026, 4, 30).unwrap(), 23, 59, 59),
        )
        .unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].1, Decimal::new(-125, 7));
        assert_eq!(rows[1].1, Decimal::new(3, 6));
    }

    #[test]
    fn funding_history_rows_bucket_to_hour_for_millisecond_offsets() {
        let rows = vec![
            (
                NanosecondTimestamp(1_777_507_200_026_000_000),
                Decimal::new(-125, 7),
            ),
            (
                NanosecondTimestamp(1_777_510_800_002_000_000),
                Decimal::new(3, 6),
            ),
        ];

        let by_hour = funding_rows_by_bucket(rows, Resolution::Hour).unwrap();

        assert_eq!(
            by_hour.get(&1_777_507_200_000_000_000),
            Some(&Decimal::new(-125, 7))
        );
        assert_eq!(
            by_hour.get(&1_777_510_800_000_000_000),
            Some(&Decimal::new(3, 6))
        );
    }

    #[test]
    fn funding_history_rows_bucket_to_daily_average() {
        let rows = vec![
            (
                NanosecondTimestamp(1_777_507_200_026_000_000),
                Decimal::new(10, 4),
            ),
            (
                NanosecondTimestamp(1_777_510_800_002_000_000),
                Decimal::new(30, 4),
            ),
        ];

        let by_day = funding_rows_by_bucket(rows, Resolution::Daily).unwrap();

        assert_eq!(
            by_day.get(&1_777_507_200_000_000_000),
            Some(&Decimal::new(20, 4))
        );
    }

    #[test]
    fn funding_cache_round_trips_and_covers_dates() {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("xyz_TSLA.csv");
        let rows = vec![
            (
                date_to_datetime(NaiveDate::from_ymd_opt(2026, 4, 30).unwrap(), 0, 0, 0),
                Decimal::new(-125, 7),
            ),
            (
                date_to_datetime(NaiveDate::from_ymd_opt(2026, 4, 30).unwrap(), 23, 0, 0),
                Decimal::new(3, 6),
            ),
        ];

        write_funding_cache(&path, &rows).unwrap();
        let read = read_funding_cache(&path).unwrap();

        assert_eq!(read, rows);
        assert!(funding_cache_covers_date(
            &read,
            NaiveDate::from_ymd_opt(2026, 4, 30).unwrap()
        ));
        assert_eq!(sanitize_cache_component("xyz:TSLA"), "xyz_TSLA");
    }

    #[test]
    fn candle_history_rows_parse_cache_and_map_to_universe_rows() {
        let response = json!([
            {
                "t": 1777507200000_i64,
                "T": 1777510799999_i64,
                "s": "xyz:TSLA",
                "i": "1h",
                "o": "250.0",
                "h": "260.0",
                "l": "249.0",
                "c": "255.0",
                "v": "10.0"
            }
        ]);
        let rows = parse_candle_history_rows(
            &response,
            date_to_datetime(NaiveDate::from_ymd_opt(2026, 4, 30).unwrap(), 0, 0, 0),
            date_to_datetime(NaiveDate::from_ymd_opt(2026, 4, 30).unwrap(), 23, 59, 59),
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        let metadata = PerpMetadata {
            coin: "xyz:TSLA".to_string(),
            max_leverage: 3,
            sz_decimals: Some(2),
            index: Some(7),
            impact_bid_ratio: Some(Decimal::new(99, 2)),
            impact_ask_ratio: Some(Decimal::new(101, 2)),
        };

        let universe_row = universe_row_from_candle(
            "HIP3_XYZ",
            "xyz",
            "xyz:TSLA",
            &rows[0],
            Some(Decimal::new(-125, 7)),
            &metadata,
        );
        assert_eq!(universe_row.symbol, "XYZ:TSLA");
        assert_eq!(universe_row.source, "info_api_candle");
        assert!(universe_row.is_historical);
        assert_eq!(universe_row.value, Some(Decimal::new(2550, 1)));
        assert_eq!(universe_row.day_ntl_vlm, Some(Decimal::new(25500, 1)));
        assert_eq!(universe_row.funding, Some(Decimal::new(-125, 7)));
        assert_eq!(universe_row.max_leverage, Some(3));
        assert_eq!(universe_row.impact_bid_px, Some(Decimal::new(25245, 2)));
        assert_eq!(universe_row.impact_ask_px, Some(Decimal::new(25755, 2)));

        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("xyz_TSLA_candles.csv");
        write_candle_cache(&path, &rows).unwrap();
        let cached = read_candle_cache(&path).unwrap();
        assert_eq!(cached, rows);
        assert!(candle_cache_covers_date(
            &cached,
            NaiveDate::from_ymd_opt(2026, 4, 30).unwrap()
        ));
        assert!(!candle_cache_covers_date(
            &cached,
            NaiveDate::from_ymd_opt(2026, 5, 1).unwrap()
        ));
        assert!(!candle_cache_covers_date(
            &[],
            NaiveDate::from_ymd_opt(2026, 4, 30).unwrap()
        ));
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
