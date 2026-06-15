use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{NaiveDate, NaiveDateTime, TimeZone, Timelike, Utc};
use lean_core::{
    DateTime, Market, NanosecondTimestamp, Resolution, SecurityType, Symbol, TickType, TimeSpan,
};
use lean_data::{
    Bar, MarginInterestRate, PerpetualContext, QuoteBar, Tick, TradeBar, TradeBarData,
};
use lean_data_providers::{
    DataType, HistoryBatchRequest, HistoryRequest, IHistoryProvider, MarketDataBatch,
};
use lean_storage::{
    custom_data_path, ParquetReader, ParquetWriter, PathResolver, QueryParams, WriterConfig,
};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::json;
use serde_json::Value;
use tracing::info;

use crate::archive::{ArchiveRuntime, S3ArchiveClient};

const HOUR_NANOS: i64 = 3_600_000_000_000;
const DEFAULT_INFO_URL: &str = "https://api.hyperliquid.xyz/info";
const MAX_CANDLES_PER_REQUEST: i64 = 5_000;
const MAX_FUNDING_ROWS_PER_REQUEST: usize = 500;
const INFO_API_MIN_REQUEST_INTERVAL: Duration = Duration::from_millis(100);
const INFO_API_MAX_RETRIES: usize = 6;
const INFO_API_RETRY_BASE_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Default)]
pub struct HyperliquidArchiveConfig {
    pub coin_map: HashMap<String, String>,
    pub info_url: Option<String>,
}

#[derive(Clone)]
pub(crate) struct HyperliquidInfoClient {
    endpoint: String,
    runtime: Arc<ArchiveRuntime>,
    next_request_at: Arc<Mutex<Instant>>,
}

impl HyperliquidInfoClient {
    pub(crate) fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            runtime: Arc::new(ArchiveRuntime::new()),
            next_request_at: Arc::new(Mutex::new(Instant::now())),
        }
    }

    fn post(&self, payload: Value) -> Result<Value> {
        for attempt in 0..=INFO_API_MAX_RETRIES {
            self.wait_for_request_slot()?;
            let endpoint = self.endpoint.clone();
            let payload = payload.clone();
            let result = self.runtime.block_on(async move {
                let response = reqwest::Client::new()
                    .post(&endpoint)
                    .json(&payload)
                    .send()
                    .await
                    .with_context(|| format!("failed to call Hyperliquid Info API {endpoint}"))?;
                let status = response.status();
                if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                    return Err(anyhow::anyhow!(
                        "Hyperliquid Info API rate limited request with HTTP 429 for {payload}"
                    ));
                }
                let response = response.error_for_status().with_context(|| {
                    format!("Hyperliquid Info API returned an error for {payload}")
                })?;
                response
                    .json::<Value>()
                    .await
                    .with_context(|| "failed to parse Hyperliquid Info API JSON response")
            })?;

            match result {
                Ok(value) => return Ok(value),
                Err(error)
                    if attempt < INFO_API_MAX_RETRIES && error.to_string().contains("HTTP 429") =>
                {
                    let multiplier = 1u32 << attempt.min(5);
                    std::thread::sleep(INFO_API_RETRY_BASE_DELAY * multiplier);
                }
                Err(error) => return Err(error),
            }
        }

        unreachable!("bounded retry loop always returns on final attempt")
    }

    fn wait_for_request_slot(&self) -> Result<()> {
        let wait = {
            let mut next = self
                .next_request_at
                .lock()
                .map_err(|_| anyhow::anyhow!("Hyperliquid Info API rate limiter lock poisoned"))?;
            let now = Instant::now();
            let scheduled = (*next).max(now);
            *next = scheduled + INFO_API_MIN_REQUEST_INTERVAL;
            scheduled.saturating_duration_since(now)
        };
        if !wait.is_zero() {
            std::thread::sleep(wait);
        }
        Ok(())
    }

    pub(crate) fn candle_snapshot(
        &self,
        coin: &str,
        interval: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Value> {
        self.post(json!({
            "type": "candleSnapshot",
            "req": {
                "coin": coin,
                "interval": interval,
                "startTime": start_ms,
                "endTime": end_ms,
            }
        }))
    }

    pub(crate) fn funding_history(&self, coin: &str, start_ms: i64, end_ms: i64) -> Result<Value> {
        self.post(json!({
            "type": "fundingHistory",
            "coin": coin,
            "startTime": start_ms,
            "endTime": end_ms,
        }))
    }

    pub(crate) fn meta_and_asset_ctxs(&self, dex: Option<&str>) -> Result<Value> {
        let mut payload = json!({ "type": "metaAndAssetCtxs" });
        if let Some(dex) = dex.map(str::trim).filter(|dex| !dex.is_empty()) {
            payload["dex"] = json!(dex);
        }
        self.post(payload)
    }

    pub(crate) fn spot_meta_and_asset_ctxs(&self) -> Result<Value> {
        self.post(json!({ "type": "spotMetaAndAssetCtxs" }))
    }
}

pub struct HyperliquidHistoryProvider {
    archive: S3ArchiveClient,
    info: HyperliquidInfoClient,
    config: HyperliquidArchiveConfig,
    resolver: PathResolver,
    reader: ParquetReader,
    writer: ParquetWriter,
}

#[derive(Debug, Clone, Copy)]
struct ImpactQuoteRatio {
    bid: Decimal,
    ask: Decimal,
}

impl HyperliquidHistoryProvider {
    pub fn new(
        data_root: impl AsRef<Path>,
        archive: S3ArchiveClient,
        config: HyperliquidArchiveConfig,
    ) -> Self {
        let info_url = config
            .info_url
            .clone()
            .unwrap_or_else(|| DEFAULT_INFO_URL.to_string());
        Self {
            archive,
            info: HyperliquidInfoClient::new(info_url),
            config,
            resolver: PathResolver::new(data_root),
            reader: ParquetReader::new(),
            writer: ParquetWriter::new(WriterConfig::default()),
        }
    }

    async fn fetch_trade_bars_from_info(
        &self,
        symbol: &Symbol,
        resolution: Resolution,
        start: DateTime,
        end: DateTime,
    ) -> Result<Vec<TradeBar>> {
        let coin = self.archive_coin(symbol)?;
        let (interval, interval_ms) = info_interval(resolution)?;
        let mut bars_by_time = BTreeMap::new();
        let mut current = start.as_millis();
        let end_ms = end.as_millis();

        while current <= end_ms {
            let chunk_end = end_ms.min(current + interval_ms * MAX_CANDLES_PER_REQUEST - 1);
            let response = self
                .info
                .candle_snapshot(&coin, interval, current, chunk_end)?;
            let bars = parse_candle_snapshot(&response, symbol, resolution, start, end)?;
            let last_bar_ms = bars.iter().map(|bar| bar.time.as_millis()).max();
            for bar in bars {
                bars_by_time.insert(bar.time.0, bar);
            }

            current = match last_bar_ms {
                Some(last) if last >= current => last + interval_ms,
                _ => chunk_end + 1,
            };
        }

        Ok(bars_by_time.into_values().collect())
    }

    async fn fetch_margin_interest_rates_from_info(
        &self,
        symbol: &Symbol,
        start: DateTime,
        end: DateTime,
    ) -> Result<Vec<MarginInterestRate>> {
        let coin = self.archive_coin(symbol)?;
        let mut rates_by_time = BTreeMap::new();
        let mut current = start.as_millis();
        let end_ms = end.as_millis();

        while current <= end_ms {
            let response = self.info.funding_history(&coin, current, end_ms)?;
            let rates = parse_funding_history(&response, symbol, start, end)?;
            let last_rate_ms = rates.iter().map(|rate| rate.time.as_millis()).max();
            let count = rates.len();
            for rate in rates {
                rates_by_time.insert(rate.time.0, rate);
            }

            current = match last_rate_ms {
                Some(last) if last >= current => last + 1,
                _ => break,
            };
            if count < MAX_FUNDING_ROWS_PER_REQUEST {
                break;
            }
        }

        Ok(rates_by_time.into_values().collect())
    }

    async fn ensure_trade_ticks(
        &self,
        symbol: &Symbol,
        start: DateTime,
        end: DateTime,
    ) -> Result<()> {
        let coin = self.archive_coin(symbol)?;
        let mut ticks = Vec::new();
        for hour in hours_in_range(start, end) {
            let key = fills_key(hour);
            let Some(text) = self.archive.fills_text(&key).await? else {
                continue;
            };
            ticks.extend(parse_fill_archive(&text, &coin, symbol)?);
        }
        self.write_ticks_by_day(symbol, TickType::Trade, &ticks)?;
        Ok(())
    }

    async fn ensure_quote_ticks(
        &self,
        symbol: &Symbol,
        start: DateTime,
        end: DateTime,
    ) -> Result<()> {
        let coin = self.archive_coin(symbol)?;
        let mut ticks = Vec::new();
        for hour in hours_in_range(start, end) {
            let key = l2_book_key(hour, &coin);
            let Some(text) = self.archive.market_text(&key).await? else {
                continue;
            };
            ticks.extend(parse_l2_book_archive(&text, &coin, symbol)?);
        }
        self.write_ticks_by_day(symbol, TickType::Quote, &ticks)?;
        Ok(())
    }

    async fn ensure_perpetual_contexts(
        &self,
        symbols: &[Symbol],
        start: DateTime,
        end: DateTime,
        quote_resolution: Resolution,
    ) -> Result<()> {
        if quote_resolution == Resolution::Tick || quote_resolution == Resolution::Second {
            return Err(anyhow::anyhow!(
                "NotImplemented: Hyperliquid asset_ctxs supports minute, hour, and daily quote bars"
            ));
        }

        let mut symbols_by_coin: HashMap<String, Vec<Symbol>> = HashMap::new();
        for symbol in symbols {
            validate_hyperliquid_symbol(symbol)?;
            if symbol.security_type() != SecurityType::CryptoFuture {
                continue;
            }
            let coin = self.archive_coin(symbol)?;
            symbols_by_coin
                .entry(archive_coin_key(&coin))
                .or_default()
                .push(symbol.clone());
        }
        if symbols_by_coin.is_empty() {
            return Ok(());
        }

        for date in dates_in_range(start, end) {
            let key = asset_contexts_key(date);
            let Some(text) = self.archive.market_text(&key).await? else {
                continue;
            };
            let parsed = parse_asset_context_archive(&text, &symbols_by_coin, start, end)
                .with_context(|| format!("failed to parse Hyperliquid asset contexts {key}"))?;
            self.write_perpetual_contexts_by_day(&parsed.contexts)?;
            self.write_margin_interest_rates_by_day(&parsed.margin_interest_rates)?;
            self.write_quote_bars_by_day(Resolution::Minute, &parsed.quote_bars)?;
            self.write_open_interest_ticks_by_day(&parsed.open_interest_ticks)?;

            if quote_resolution != Resolution::Minute {
                let aggregated = aggregate_quote_bars(&parsed.quote_bars, quote_resolution)?;
                self.write_quote_bars_by_day(quote_resolution, &aggregated)?;
            }
        }

        Ok(())
    }

    fn archive_coin(&self, symbol: &Symbol) -> Result<String> {
        validate_hyperliquid_symbol(symbol)?;
        let key = symbol.value.trim().to_ascii_uppercase();
        if let Some(mapped) = self.config.coin_map.get(&key) {
            return Ok(mapped.clone());
        }
        if let Some((dex, coin)) = key.split_once(':') {
            let coin = strip_quote_suffix(coin);
            let coin = default_archive_coin_alias_for_dex(dex, &coin)
                .map(str::to_string)
                .unwrap_or(coin);
            return Ok(format!("{}:{coin}", dex.to_ascii_lowercase()));
        }
        if key.starts_with('@') {
            return Ok(key);
        }
        if let Some(mapped) = default_archive_coin_alias(&key) {
            return Ok(mapped.to_string());
        }
        Ok(strip_quote_suffix(&key))
    }

    fn read_ticks(
        &self,
        symbol: &Symbol,
        tick_type: TickType,
        start: DateTime,
        end: DateTime,
    ) -> Result<Vec<Tick>> {
        let mut ticks = Vec::new();
        let params = QueryParams::new()
            .with_time_range(start, end)
            .with_symbols(vec![symbol.id.sid]);
        for date in dates_in_range(start, end) {
            let path =
                self.resolver
                    .market_data_partition(symbol, Resolution::Tick, tick_type, date);
            ticks.extend(self.reader.read_tick_partition(&path, symbol, &params)?);
        }
        ticks.sort_by_key(|tick| tick.time.0);
        Ok(ticks)
    }

    fn read_margin_interest_rates(
        &self,
        symbol: &Symbol,
        start: DateTime,
        end: DateTime,
    ) -> Result<Vec<MarginInterestRate>> {
        let mut rates = Vec::new();
        let params = QueryParams::new()
            .with_time_range(start, end)
            .with_symbols(vec![symbol.id.sid]);
        for date in dates_in_range(start, end) {
            let path = self.resolver.margin_interest_partition(symbol, date);
            rates.extend(
                self.reader
                    .read_margin_interest_rate_partition(&path, symbol, &params)?,
            );
        }
        rates.sort_by_key(|rate| rate.time.0);
        Ok(rates)
    }

    fn read_quote_bars(
        &self,
        symbol: &Symbol,
        resolution: Resolution,
        start: DateTime,
        end: DateTime,
    ) -> Result<Vec<QuoteBar>> {
        let mut bars = Vec::new();
        let params = QueryParams::new()
            .with_time_range(start, end)
            .with_symbols(vec![symbol.id.sid]);
        for date in dates_in_range(start, end) {
            let path =
                self.resolver
                    .market_data_partition(symbol, resolution, TickType::Quote, date);
            bars.extend(
                self.reader
                    .read_quote_bar_partition(&path, symbol, &params)?,
            );
        }
        bars.sort_by_key(|bar| bar.time.0);
        Ok(bars)
    }

    fn read_trade_bars(
        &self,
        symbol: &Symbol,
        resolution: Resolution,
        start: DateTime,
        end: DateTime,
    ) -> Result<Vec<TradeBar>> {
        let mut bars = Vec::new();
        let params = QueryParams::new()
            .with_time_range(start, end)
            .with_symbols(vec![symbol.id.sid]);
        for date in dates_in_range(start, end) {
            let path =
                self.resolver
                    .market_data_partition(symbol, resolution, TickType::Trade, date);
            bars.extend(
                self.reader
                    .read_trade_bar_partition(&path, symbol, &params)?,
            );
        }
        bars.sort_by_key(|bar| bar.time.0);
        Ok(bars)
    }

    async fn read_or_fetch_trade_bars_from_info(
        &self,
        symbols: &[Symbol],
        resolution: Resolution,
        start: DateTime,
        end: DateTime,
    ) -> Result<Vec<TradeBar>> {
        let mut output = Vec::new();
        for symbol in symbols {
            validate_hyperliquid_symbol(symbol)?;
            let cached = self.read_trade_bars(symbol, resolution, start, end)?;
            if !cached.is_empty() {
                output.extend(cached);
                continue;
            }

            let fetched = self
                .fetch_trade_bars_from_info(symbol, resolution, start, end)
                .await?;
            self.write_trade_bars_by_day(symbol, resolution, &fetched)?;
            if fetched.is_empty() && is_hip3_symbol(symbol) {
                self.ensure_hip3_market_data_from_custom_universe(
                    std::slice::from_ref(symbol),
                    resolution,
                    start,
                    end,
                )?;
                output.extend(self.read_trade_bars(symbol, resolution, start, end)?);
            } else {
                output.extend(fetched);
            }
        }
        sort_and_dedupe_trade_bars(&mut output);
        Ok(output)
    }

    fn ensure_hip3_market_data_from_custom_universe(
        &self,
        symbols: &[Symbol],
        resolution: Resolution,
        start: DateTime,
        end: DateTime,
    ) -> Result<()> {
        if matches!(resolution, Resolution::Tick | Resolution::Second) {
            return Ok(());
        }

        let mut by_ticker: HashMap<String, Vec<(String, Symbol)>> = HashMap::new();
        for symbol in symbols.iter().filter(|symbol| is_hip3_symbol(symbol)) {
            let coin = self.archive_coin(symbol)?;
            let Some((dex, _)) = coin.split_once(':') else {
                continue;
            };
            by_ticker
                .entry(format!("HIP3_{}", dex.to_ascii_uppercase()))
                .or_default()
                .push((archive_coin_key(&coin), symbol.clone()));
        }
        if by_ticker.is_empty() {
            return Ok(());
        }

        let mut minute_trade_bars = Vec::new();
        let mut minute_quote_bars = Vec::new();
        let mut contexts = Vec::new();
        let mut margin_rates = Vec::new();
        let mut open_interest_ticks = Vec::new();

        for date in dates_in_range(start, end) {
            for (ticker, requested_symbols) in &by_ticker {
                let path = custom_data_path(&self.resolver.data_root, "hyperliquid", ticker, date);
                let points = self.reader.read_custom_data_points(&path)?;
                for point in points {
                    let Some(time) = point.end_time else {
                        continue;
                    };
                    if time < start || time > end {
                        continue;
                    }
                    let row_symbol = custom_string_field(&point, "symbol")
                        .map(|value| value.to_ascii_uppercase())
                        .unwrap_or_default();
                    let row_coin = custom_string_field(&point, "coin")
                        .as_deref()
                        .map(archive_coin_key)
                        .unwrap_or_default();
                    let source = custom_string_field(&point, "source").unwrap_or_default();
                    if !matches!(
                        source.as_str(),
                        "asset_ctxs" | "info_api_candle" | "node_fills_by_block"
                    ) {
                        continue;
                    }

                    for (coin_key, symbol) in requested_symbols {
                        if row_coin != *coin_key && row_symbol != symbol.value {
                            continue;
                        }
                        let Some(price) = custom_decimal_field(&point, "mid_px")
                            .or_else(|| custom_decimal_field(&point, "mark_px"))
                            .or_else(|| Some(point.value))
                            .filter(|price| *price > Decimal::ZERO)
                        else {
                            continue;
                        };

                        minute_trade_bars.push(TradeBar::new(
                            symbol.clone(),
                            time,
                            TimeSpan::ONE_MINUTE,
                            TradeBarData::new(price, price, price, price, Decimal::ZERO),
                        ));

                        let funding =
                            custom_decimal_field(&point, "funding").unwrap_or(Decimal::ZERO);
                        let open_interest =
                            custom_decimal_field(&point, "open_interest").unwrap_or(Decimal::ZERO);
                        let prev_day_px =
                            custom_decimal_field(&point, "prev_day_px").unwrap_or(Decimal::ZERO);
                        let day_ntl_vlm =
                            custom_decimal_field(&point, "day_ntl_vlm").unwrap_or(Decimal::ZERO);
                        let premium =
                            custom_decimal_field(&point, "premium").unwrap_or(Decimal::ZERO);
                        let oracle_px =
                            custom_decimal_field(&point, "oracle_px").unwrap_or(Decimal::ZERO);
                        let mark_px = custom_decimal_field(&point, "mark_px").unwrap_or(price);
                        let mid_px = custom_decimal_field(&point, "mid_px").unwrap_or(price);
                        let impact_bid_px =
                            custom_decimal_field(&point, "impact_bid_px").unwrap_or(Decimal::ZERO);
                        let impact_ask_px =
                            custom_decimal_field(&point, "impact_ask_px").unwrap_or(Decimal::ZERO);

                        let context = PerpetualContext::new(
                            symbol.clone(),
                            time,
                            TimeSpan::ONE_MINUTE,
                            funding,
                            open_interest,
                            prev_day_px,
                            day_ntl_vlm,
                            premium,
                            oracle_px,
                            mark_px,
                            mid_px,
                            impact_bid_px,
                            impact_ask_px,
                        );
                        if let Some(quote_bar) = quote_bar_from_perpetual_context(&context) {
                            minute_quote_bars.push(quote_bar);
                        }
                        margin_rates.push(MarginInterestRate::new(symbol.clone(), time, funding));
                        if open_interest > Decimal::ZERO {
                            open_interest_ticks.push(Tick::open_interest(
                                symbol.clone(),
                                time,
                                open_interest,
                            ));
                        }
                        contexts.push(context);
                    }
                }
            }
        }

        sort_and_dedupe_trade_bars(&mut minute_trade_bars);
        sort_and_dedupe_quote_bars(&mut minute_quote_bars);
        sort_and_dedupe_margin_interest_rates(&mut margin_rates);
        sort_and_dedupe_perpetual_contexts(&mut contexts);
        sort_and_dedupe_ticks(&mut open_interest_ticks);
        let trade_bars = aggregate_trade_bars(&minute_trade_bars, resolution)?;
        let quote_bars = aggregate_quote_bars(&minute_quote_bars, resolution)?;
        self.write_trade_bars_by_day_for_all_symbols(resolution, &trade_bars)?;
        self.write_quote_bars_by_day(resolution, &quote_bars)?;
        self.write_perpetual_contexts_by_day(&contexts)?;
        self.write_margin_interest_rates_by_day(&margin_rates)?;
        self.write_open_interest_ticks_by_day(&open_interest_ticks)?;
        Ok(())
    }

    async fn ensure_hip3_quote_bars_from_trade_bars(
        &self,
        symbols: &[Symbol],
        resolution: Resolution,
        start: DateTime,
        end: DateTime,
    ) -> Result<()> {
        let hip3_symbols = symbols
            .iter()
            .filter(|symbol| is_hip3_symbol(symbol))
            .cloned()
            .collect::<Vec<_>>();
        if hip3_symbols.is_empty() {
            return Ok(());
        }

        let ratios = self.current_impact_quote_ratios(&hip3_symbols)?;
        if ratios.is_empty() {
            return Ok(());
        }

        let mut trade_bars_by_sid: HashMap<u64, Vec<TradeBar>> = HashMap::new();
        if resolution == Resolution::Minute {
            let mut missing_symbols = Vec::new();
            for symbol in &hip3_symbols {
                let cached = self.read_trade_bars(symbol, resolution, start, end)?;
                if cached.is_empty() {
                    missing_symbols.push(symbol.clone());
                } else {
                    trade_bars_by_sid.insert(symbol.id.sid, cached);
                }
            }

            if !missing_symbols.is_empty() {
                let bars = self
                    .read_or_fetch_trade_bars_from_info(&missing_symbols, resolution, start, end)
                    .await?;
                for bar in bars {
                    trade_bars_by_sid
                        .entry(bar.symbol.id.sid)
                        .or_default()
                        .push(bar);
                }
            }
        }

        let mut quote_bars = Vec::new();
        for symbol in &hip3_symbols {
            let coin_key = archive_coin_key(&self.archive_coin(symbol)?);
            let Some(ratio) = ratios.get(&coin_key).copied() else {
                continue;
            };

            let trade_bars = if let Some(bars) = trade_bars_by_sid.get(&symbol.id.sid) {
                bars.clone()
            } else {
                let cached = self.read_trade_bars(symbol, resolution, start, end)?;
                if !cached.is_empty() {
                    cached
                } else {
                    self.read_or_fetch_trade_bars_from_info(
                        std::slice::from_ref(symbol),
                        resolution,
                        start,
                        end,
                    )
                    .await?
                }
            };

            quote_bars.extend(
                trade_bars
                    .iter()
                    .filter_map(|bar| quote_bar_from_trade_bar_with_impact_ratio(bar, ratio)),
            );
        }

        sort_and_dedupe_quote_bars(&mut quote_bars);
        self.write_quote_bars_by_day(resolution, &quote_bars)
    }

    fn current_impact_quote_ratios(
        &self,
        symbols: &[Symbol],
    ) -> Result<HashMap<String, ImpactQuoteRatio>> {
        let mut requested_by_dex: HashMap<String, HashSet<String>> = HashMap::new();
        for symbol in symbols {
            let coin = self.archive_coin(symbol)?;
            let Some((dex, _)) = coin.split_once(':') else {
                continue;
            };
            requested_by_dex
                .entry(dex.to_ascii_lowercase())
                .or_default()
                .insert(archive_coin_key(&coin));
        }

        let mut ratios = HashMap::new();
        for (dex, requested) in requested_by_dex {
            let response = self.info.meta_and_asset_ctxs(Some(&dex))?;
            let Some(array) = response.as_array().filter(|array| array.len() >= 2) else {
                continue;
            };
            let Some(universe) = array[0].get("universe").and_then(Value::as_array) else {
                continue;
            };
            let Some(contexts) = array[1].as_array() else {
                continue;
            };

            for (index, asset) in universe.iter().enumerate() {
                let Some(name) = asset.get("name").and_then(Value::as_str).map(str::trim) else {
                    continue;
                };
                let coin_key = archive_coin_key(name);
                if !requested.contains(&coin_key) {
                    continue;
                }
                let Some(context) = contexts.get(index) else {
                    continue;
                };
                let Some(mid) =
                    decimal_field(context, "midPx").or_else(|| decimal_field(context, "markPx"))
                else {
                    continue;
                };
                let Some(impact_pxs) = context.get("impactPxs").and_then(Value::as_array) else {
                    continue;
                };
                let Some(first) = impact_pxs.first().and_then(parse_decimal) else {
                    continue;
                };
                let Some(second) = impact_pxs.get(1).and_then(parse_decimal) else {
                    continue;
                };
                if mid <= Decimal::ZERO || first <= Decimal::ZERO || second <= Decimal::ZERO {
                    continue;
                }
                let bid = first.min(second);
                let ask = first.max(second);
                if ask < bid {
                    continue;
                }
                ratios.insert(
                    coin_key,
                    ImpactQuoteRatio {
                        bid: bid / mid,
                        ask: ask / mid,
                    },
                );
            }
        }

        Ok(ratios)
    }

    fn read_perpetual_contexts(
        &self,
        symbol: &Symbol,
        start: DateTime,
        end: DateTime,
    ) -> Result<Vec<PerpetualContext>> {
        let mut contexts = Vec::new();
        let params = QueryParams::new()
            .with_time_range(start, end)
            .with_symbols(vec![symbol.id.sid]);
        for date in dates_in_range(start, end) {
            let path = self.resolver.perpetual_context_partition(symbol, date);
            contexts.extend(
                self.reader
                    .read_perpetual_context_partition(&path, symbol, &params)?,
            );
        }
        contexts.sort_by_key(|context| context.time.0);
        Ok(contexts)
    }

    fn read_existing_ticks_for_day(
        &self,
        symbol: &Symbol,
        tick_type: TickType,
        date: NaiveDate,
    ) -> Result<Vec<Tick>> {
        let path = self
            .resolver
            .market_data_partition(symbol, Resolution::Tick, tick_type, date);
        let params = QueryParams::new().with_symbols(vec![symbol.id.sid]);
        Ok(self.reader.read_tick_partition(&path, symbol, &params)?)
    }

    fn write_ticks_by_day(
        &self,
        symbol: &Symbol,
        tick_type: TickType,
        new_ticks: &[Tick],
    ) -> Result<()> {
        if new_ticks.is_empty() {
            return Ok(());
        }

        let mut by_date: BTreeMap<NaiveDate, Vec<Tick>> = BTreeMap::new();
        for tick in new_ticks {
            by_date
                .entry(tick.time.date_utc())
                .or_default()
                .push(tick.clone());
        }

        for (date, mut ticks) in by_date {
            ticks.extend(self.read_existing_ticks_for_day(symbol, tick_type, date)?);
            sort_and_dedupe_ticks(&mut ticks);
            let path =
                self.resolver
                    .market_data_partition(symbol, Resolution::Tick, tick_type, date);
            self.writer.merge_tick_partition(&ticks, &path)?;
            info!(
                "Hyperliquid: cached {} {:?} ticks to {}",
                ticks.len(),
                tick_type,
                path.display()
            );
        }
        Ok(())
    }

    fn write_trade_bars_by_day(
        &self,
        symbol: &Symbol,
        resolution: Resolution,
        bars: &[TradeBar],
    ) -> Result<()> {
        if bars.is_empty() {
            return Ok(());
        }
        let mut by_date: BTreeMap<NaiveDate, Vec<TradeBar>> = BTreeMap::new();
        for bar in bars {
            by_date
                .entry(bar.time.date_utc())
                .or_default()
                .push(bar.clone());
        }
        for (date, mut bars) in by_date {
            let path =
                self.resolver
                    .market_data_partition(symbol, resolution, TickType::Trade, date);
            let params = QueryParams::new().with_symbols(vec![symbol.id.sid]);
            bars.extend(
                self.reader
                    .read_trade_bar_partition(&path, symbol, &params)?,
            );
            sort_and_dedupe_trade_bars(&mut bars);
            self.writer.merge_trade_bar_partition(&bars, &path)?;
            info!(
                "Hyperliquid: cached {} trade bars to {}",
                bars.len(),
                path.display()
            );
        }
        Ok(())
    }

    fn write_trade_bars_by_day_for_all_symbols(
        &self,
        resolution: Resolution,
        bars: &[TradeBar],
    ) -> Result<()> {
        if bars.is_empty() {
            return Ok(());
        }
        let mut by_date: BTreeMap<NaiveDate, Vec<TradeBar>> = BTreeMap::new();
        for bar in bars {
            by_date
                .entry(bar.time.date_utc())
                .or_default()
                .push(bar.clone());
        }
        for (date, mut bars) in by_date {
            sort_and_dedupe_trade_bars(&mut bars);
            let path = self.resolver.market_data_partition(
                &bars[0].symbol,
                resolution,
                TickType::Trade,
                date,
            );
            self.writer.merge_trade_bar_partition(&bars, &path)?;
            info!(
                "Hyperliquid: cached {} trade bars to {}",
                bars.len(),
                path.display()
            );
        }
        Ok(())
    }

    fn write_quote_bars_by_day(&self, resolution: Resolution, bars: &[QuoteBar]) -> Result<()> {
        if bars.is_empty() {
            return Ok(());
        }
        let mut by_date: BTreeMap<NaiveDate, Vec<QuoteBar>> = BTreeMap::new();
        for bar in bars {
            by_date
                .entry(bar.time.date_utc())
                .or_default()
                .push(bar.clone());
        }
        for (date, mut bars) in by_date {
            sort_and_dedupe_quote_bars(&mut bars);
            let path = self.resolver.market_data_partition(
                &bars[0].symbol,
                resolution,
                TickType::Quote,
                date,
            );
            self.writer.merge_quote_bar_partition(&bars, &path)?;
            info!(
                "Hyperliquid: cached {} quote bars to {}",
                bars.len(),
                path.display()
            );
        }
        Ok(())
    }

    fn write_open_interest_ticks_by_day(&self, ticks: &[Tick]) -> Result<()> {
        if ticks.is_empty() {
            return Ok(());
        }
        let mut by_symbol: HashMap<u64, Vec<Tick>> = HashMap::new();
        for tick in ticks {
            by_symbol
                .entry(tick.symbol.id.sid)
                .or_default()
                .push(tick.clone());
        }
        for ticks in by_symbol.values() {
            self.write_ticks_by_day(&ticks[0].symbol, TickType::OpenInterest, ticks)?;
        }
        Ok(())
    }

    fn write_perpetual_contexts_by_day(&self, contexts: &[PerpetualContext]) -> Result<()> {
        if contexts.is_empty() {
            return Ok(());
        }
        let mut by_date: BTreeMap<NaiveDate, Vec<PerpetualContext>> = BTreeMap::new();
        for context in contexts {
            by_date
                .entry(context.time.date_utc())
                .or_default()
                .push(context.clone());
        }
        for (date, mut contexts) in by_date {
            sort_and_dedupe_perpetual_contexts(&mut contexts);
            let path = self
                .resolver
                .perpetual_context_partition(&contexts[0].symbol, date);
            self.writer
                .merge_perpetual_context_partition(&contexts, &path)?;
            info!(
                "Hyperliquid: cached {} perpetual context rows to {}",
                contexts.len(),
                path.display()
            );
        }
        Ok(())
    }

    fn write_margin_interest_rates_by_day(&self, rates: &[MarginInterestRate]) -> Result<()> {
        if rates.is_empty() {
            return Ok(());
        }
        let mut by_date: BTreeMap<NaiveDate, Vec<MarginInterestRate>> = BTreeMap::new();
        for rate in rates {
            by_date
                .entry(rate.time.date_utc())
                .or_default()
                .push(rate.clone());
        }
        for (date, mut rates) in by_date {
            sort_and_dedupe_margin_interest_rates(&mut rates);
            let path = self
                .resolver
                .margin_interest_partition(&rates[0].symbol, date);
            self.writer
                .merge_margin_interest_rate_partition(&rates, &path)?;
            info!(
                "Hyperliquid: cached {} funding rates to {}",
                rates.len(),
                path.display()
            );
        }
        Ok(())
    }
}

#[async_trait]
impl IHistoryProvider for HyperliquidHistoryProvider {
    async fn get_history(&self, request: &HistoryRequest) -> Result<Vec<TradeBar>> {
        if request.data_type != DataType::TradeBar {
            return Err(anyhow::anyhow!(
                "NotImplemented: Hyperliquid expected TradeBar request, got {:?}",
                request.data_type
            ));
        }
        validate_hyperliquid_symbol(&request.symbol)?;
        if request.resolution == Resolution::Tick {
            return Err(anyhow::anyhow!(
                "NotImplemented: Hyperliquid trade bars require a bar resolution"
            ));
        }
        if request.resolution == Resolution::Second {
            return Err(anyhow::anyhow!(
                "NotImplemented: Hyperliquid Info API does not provide second trade bars"
            ));
        }

        let bars = self
            .read_or_fetch_trade_bars_from_info(
                std::slice::from_ref(&request.symbol),
                request.resolution,
                request.start,
                request.end,
            )
            .await?;
        if !bars.is_empty() && request.symbol.security_type() == SecurityType::CryptoFuture {
            self.ensure_perpetual_contexts(
                std::slice::from_ref(&request.symbol),
                request.start,
                request.end,
                request.resolution,
            )
            .await?;
        }
        Ok(bars)
    }

    async fn get_quote_bars(&self, request: &HistoryRequest) -> Result<Vec<QuoteBar>> {
        if request.data_type != DataType::QuoteBar {
            return Err(anyhow::anyhow!(
                "NotImplemented: Hyperliquid expected QuoteBar request, got {:?}",
                request.data_type
            ));
        }
        validate_hyperliquid_symbol(&request.symbol)?;
        if request.resolution == Resolution::Tick {
            return Err(anyhow::anyhow!(
                "NotImplemented: Hyperliquid quote bars require a bar resolution"
            ));
        }
        if request.resolution == Resolution::Second {
            return Err(anyhow::anyhow!(
                "NotImplemented: Hyperliquid asset_ctxs does not provide second quote bars"
            ));
        }
        self.ensure_perpetual_contexts(
            std::slice::from_ref(&request.symbol),
            request.start,
            request.end,
            request.resolution,
        )
        .await?;
        let mut bars = self.read_quote_bars(
            &request.symbol,
            request.resolution,
            request.start,
            request.end,
        )?;
        if bars.is_empty() && is_hip3_symbol(&request.symbol) {
            self.ensure_hip3_quote_bars_from_trade_bars(
                std::slice::from_ref(&request.symbol),
                request.resolution,
                request.start,
                request.end,
            )
            .await?;
            bars = self.read_quote_bars(
                &request.symbol,
                request.resolution,
                request.start,
                request.end,
            )?;
        }
        Ok(bars)
    }

    async fn get_ticks(&self, request: &HistoryRequest) -> Result<Vec<Tick>> {
        if request.data_type != DataType::Tick {
            return Err(anyhow::anyhow!(
                "NotImplemented: Hyperliquid expected Tick request, got {:?}",
                request.data_type
            ));
        }
        validate_hyperliquid_symbol(&request.symbol)?;

        self.ensure_trade_ticks(&request.symbol, request.start, request.end)
            .await?;
        self.ensure_quote_ticks(&request.symbol, request.start, request.end)
            .await?;

        let mut ticks =
            self.read_ticks(&request.symbol, TickType::Trade, request.start, request.end)?;
        ticks.extend(self.read_ticks(
            &request.symbol,
            TickType::Quote,
            request.start,
            request.end,
        )?);
        ticks.sort_by_key(|tick| tick.time.0);
        Ok(ticks)
    }

    async fn get_margin_interest_rates(
        &self,
        request: &HistoryRequest,
    ) -> Result<Vec<MarginInterestRate>> {
        if request.data_type != DataType::MarginInterestRate {
            return Err(anyhow::anyhow!(
                "NotImplemented: Hyperliquid expected MarginInterestRate request, got {:?}",
                request.data_type
            ));
        }
        validate_hyperliquid_symbol(&request.symbol)?;
        if request.symbol.security_type() != SecurityType::CryptoFuture {
            return Err(anyhow::anyhow!(
                "NotImplemented: Hyperliquid funding rates require CryptoFuture symbols"
            ));
        }

        if is_hip3_symbol(&request.symbol) {
            let fetch_end = funding_history_cache_end(request.end);
            let rates = self
                .fetch_margin_interest_rates_from_info(&request.symbol, request.start, fetch_end)
                .await?;
            self.write_margin_interest_rates_by_day(&rates)?;
            return self.read_margin_interest_rates(&request.symbol, request.start, request.end);
        }

        self.ensure_perpetual_contexts(
            std::slice::from_ref(&request.symbol),
            request.start,
            request.end,
            Resolution::Minute,
        )
        .await?;
        self.read_margin_interest_rates(&request.symbol, request.start, request.end)
    }

    async fn get_perpetual_contexts(
        &self,
        request: &HistoryRequest,
    ) -> Result<Vec<PerpetualContext>> {
        if request.data_type != DataType::PerpetualContext {
            return Err(anyhow::anyhow!(
                "NotImplemented: Hyperliquid expected PerpetualContext request, got {:?}",
                request.data_type
            ));
        }
        validate_hyperliquid_symbol(&request.symbol)?;
        if request.symbol.security_type() != SecurityType::CryptoFuture {
            return Err(anyhow::anyhow!(
                "NotImplemented: Hyperliquid perpetual contexts require CryptoFuture symbols"
            ));
        }
        if request.resolution != Resolution::Minute {
            return Err(anyhow::anyhow!(
                "NotImplemented: Hyperliquid asset_ctxs is minute-resolution context data"
            ));
        }

        self.ensure_perpetual_contexts(
            std::slice::from_ref(&request.symbol),
            request.start,
            request.end,
            Resolution::Minute,
        )
        .await?;
        self.read_perpetual_contexts(&request.symbol, request.start, request.end)
    }

    async fn get_history_batch(&self, request: &HistoryBatchRequest) -> Result<MarketDataBatch> {
        match request.data_type {
            DataType::TradeBar if request.resolution == Resolution::Minute => {
                let bars = self
                    .read_or_fetch_trade_bars_from_info(
                        &request.symbols,
                        request.resolution,
                        request.start,
                        request.end,
                    )
                    .await?;
                if !bars.is_empty() {
                    let symbols = request
                        .symbols
                        .iter()
                        .filter(|symbol| symbol.security_type() == SecurityType::CryptoFuture)
                        .cloned()
                        .collect::<Vec<_>>();
                    if !symbols.is_empty() {
                        self.ensure_perpetual_contexts(
                            &symbols,
                            request.start,
                            request.end,
                            request.resolution,
                        )
                        .await?;
                    }
                }
                Ok(MarketDataBatch {
                    trade_bars: bars,
                    ..MarketDataBatch::default()
                })
            }
            DataType::PerpetualContext => {
                if request.resolution != Resolution::Minute {
                    return Err(anyhow::anyhow!(
                        "NotImplemented: Hyperliquid asset_ctxs is minute-resolution context data"
                    ));
                }
                self.ensure_perpetual_contexts(
                    &request.symbols,
                    request.start,
                    request.end,
                    Resolution::Minute,
                )
                .await?;
                let mut batch = MarketDataBatch::default();
                for symbol in &request.symbols {
                    batch
                        .perpetual_contexts
                        .extend(self.read_perpetual_contexts(
                            symbol,
                            request.start,
                            request.end,
                        )?);
                }
                Ok(batch)
            }
            DataType::QuoteBar => {
                self.ensure_perpetual_contexts(
                    &request.symbols,
                    request.start,
                    request.end,
                    request.resolution,
                )
                .await?;
                let mut batch = MarketDataBatch::default();
                let mut missing_hip3_symbols = Vec::new();
                for symbol in &request.symbols {
                    let bars = self.read_quote_bars(
                        symbol,
                        request.resolution,
                        request.start,
                        request.end,
                    )?;
                    if bars.is_empty() && is_hip3_symbol(symbol) {
                        missing_hip3_symbols.push(symbol.clone());
                    }
                    batch.quote_bars.extend(bars);
                }
                if !missing_hip3_symbols.is_empty() {
                    self.ensure_hip3_quote_bars_from_trade_bars(
                        &missing_hip3_symbols,
                        request.resolution,
                        request.start,
                        request.end,
                    )
                    .await?;
                    for symbol in &missing_hip3_symbols {
                        batch.quote_bars.extend(self.read_quote_bars(
                            symbol,
                            request.resolution,
                            request.start,
                            request.end,
                        )?);
                    }
                }
                Ok(batch)
            }
            _ => {
                let mut batch = MarketDataBatch::default();
                for symbol in &request.symbols {
                    let single = HistoryRequest {
                        symbol: symbol.clone(),
                        resolution: request.resolution,
                        start: request.start,
                        end: request.end,
                        data_type: request.data_type,
                    };
                    match request.data_type {
                        DataType::TradeBar | DataType::FactorFile | DataType::MapFile => {
                            batch.trade_bars.extend(self.get_history(&single).await?);
                        }
                        DataType::Tick | DataType::OpenInterest => {
                            batch.ticks.extend(self.get_ticks(&single).await?);
                        }
                        DataType::MarginInterestRate => {
                            batch
                                .margin_interest_rates
                                .extend(self.get_margin_interest_rates(&single).await?);
                        }
                        DataType::QuoteBar | DataType::PerpetualContext => unreachable!(),
                    }
                }
                Ok(batch)
            }
        }
    }

    fn earliest_date(&self) -> Option<NaiveDate> {
        NaiveDate::from_ymd_opt(2023, 1, 1)
    }
}

fn validate_hyperliquid_symbol(symbol: &Symbol) -> Result<()> {
    if symbol.market().as_str() != Market::HYPERLIQUID {
        return Err(anyhow::anyhow!(
            "NotImplemented: Hyperliquid only supports Market.Hyperliquid symbols"
        ));
    }
    if !matches!(
        symbol.security_type(),
        SecurityType::Crypto | SecurityType::CryptoFuture
    ) {
        return Err(anyhow::anyhow!(
            "NotImplemented: Hyperliquid only supports Crypto and CryptoFuture symbols"
        ));
    }
    Ok(())
}

fn is_hip3_symbol(symbol: &Symbol) -> bool {
    symbol.security_type() == SecurityType::CryptoFuture && symbol.value.trim().contains(':')
}

fn funding_history_cache_end(request_end: DateTime) -> DateTime {
    let now = DateTime::from(Utc::now());
    if now > request_end {
        now
    } else {
        request_end
    }
}

fn strip_quote_suffix(symbol: &str) -> String {
    for suffix in ["-PERP", "PERP", "USDC", "USDT", "USD"] {
        if symbol.ends_with(suffix) && symbol.len() > suffix.len() {
            return symbol[..symbol.len() - suffix.len()].to_string();
        }
    }
    symbol.to_string()
}

fn default_archive_coin_alias(symbol: &str) -> Option<&'static str> {
    match symbol {
        // trading.xyz's S&P 500 proxy is a HIP-3 market, not the core SPX perp.
        "USA500" | "USA500USD" | "USA500USDC" | "USA500USDT" | "SP500" | "SP500USD"
        | "SP500USDC" | "SP500USDT" => Some("xyz:SP500"),
        // Hyperliquid coin names are case-sensitive for kilo-denominated listings.
        "KPEPE" => Some("kPEPE"),
        "KSHIB" => Some("kSHIB"),
        "KBONK" => Some("kBONK"),
        "KLUNC" => Some("kLUNC"),
        "KFLOKI" => Some("kFLOKI"),
        "KDOGS" => Some("kDOGS"),
        "KNEIRO" => Some("kNEIRO"),
        _ => None,
    }
}

fn default_archive_coin_alias_for_dex(dex: &str, symbol: &str) -> Option<&'static str> {
    match (dex.to_ascii_lowercase().as_str(), symbol) {
        ("xyz", "USA500" | "USA500USD" | "USA500USDC" | "USA500USDT") => Some("SP500"),
        _ => default_archive_coin_alias(symbol).filter(|mapped| !mapped.contains(':')),
    }
}

fn hours_in_range(start: DateTime, end: DateTime) -> Vec<chrono::DateTime<Utc>> {
    if end < start {
        return Vec::new();
    }
    let mut current = start.0.div_euclid(HOUR_NANOS) * HOUR_NANOS;
    let final_hour = end.0.div_euclid(HOUR_NANOS) * HOUR_NANOS;
    let mut hours = Vec::new();
    while current <= final_hour {
        hours.push(nanos_to_utc(current));
        current += HOUR_NANOS;
    }
    hours
}

fn dates_in_range(start: DateTime, end: DateTime) -> Vec<NaiveDate> {
    if end < start {
        return Vec::new();
    }
    let mut current = start.date_utc();
    let final_date = end.date_utc();
    let mut dates = Vec::new();
    while current <= final_date {
        dates.push(current);
        current = current.succ_opt().expect("date range overflow");
    }
    dates
}

fn nanos_to_utc(ns: i64) -> chrono::DateTime<Utc> {
    let secs = ns.div_euclid(1_000_000_000);
    let nanos = ns.rem_euclid(1_000_000_000) as u32;
    Utc.timestamp_opt(secs, nanos).unwrap()
}

fn l2_book_key(hour: chrono::DateTime<Utc>, coin: &str) -> String {
    format!(
        "market_data/{}/{}/l2Book/{}.lz4",
        hour.format("%Y%m%d"),
        hour.hour(),
        coin.to_ascii_uppercase()
    )
}

pub(crate) fn fills_key(hour: chrono::DateTime<Utc>) -> String {
    format!(
        "node_fills_by_block/hourly/{}/{}.lz4",
        hour.format("%Y%m%d"),
        hour.hour()
    )
}

pub(crate) fn asset_contexts_key(date: NaiveDate) -> String {
    format!("asset_ctxs/{}.csv.lz4", date.format("%Y%m%d"))
}

pub(crate) fn archive_coin_key(coin: &str) -> String {
    coin.trim().to_ascii_uppercase()
}

#[derive(Debug, Default)]
struct AssetContextDownload {
    contexts: Vec<PerpetualContext>,
    margin_interest_rates: Vec<MarginInterestRate>,
    quote_bars: Vec<QuoteBar>,
    open_interest_ticks: Vec<Tick>,
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

fn parse_asset_context_archive(
    text: &str,
    symbols_by_coin: &HashMap<String, Vec<Symbol>>,
    start: DateTime,
    end: DateTime,
) -> Result<AssetContextDownload> {
    let mut download = AssetContextDownload::default();
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(text.as_bytes());

    for row in reader.deserialize::<AssetContextCsvRow>() {
        let row = row.context("failed to parse Hyperliquid asset_ctxs CSV row")?;
        let Some(symbols) = symbols_by_coin.get(&archive_coin_key(&row.coin)) else {
            continue;
        };
        let time = parse_timestamp(&Value::String(row.time.clone())).with_context(|| {
            format!(
                "failed to parse Hyperliquid asset_ctxs timestamp {}",
                row.time
            )
        })?;
        if time < start || time > end {
            continue;
        }

        let funding = parse_decimal_str(&row.funding, "funding")?;
        let open_interest = parse_decimal_str(&row.open_interest, "open_interest")?;
        let prev_day_px = parse_decimal_str(&row.prev_day_px, "prev_day_px")?;
        let day_ntl_vlm = parse_decimal_str(&row.day_ntl_vlm, "day_ntl_vlm")?;
        let premium = parse_decimal_str(&row.premium, "premium")?;
        let oracle_px = parse_decimal_str(&row.oracle_px, "oracle_px")?;
        let mark_px = parse_decimal_str(&row.mark_px, "mark_px")?;
        let mid_px = parse_decimal_str(&row.mid_px, "mid_px")?;
        let impact_bid_px = parse_decimal_str(&row.impact_bid_px, "impact_bid_px")?;
        let impact_ask_px = parse_decimal_str(&row.impact_ask_px, "impact_ask_px")?;

        for symbol in symbols {
            let context = PerpetualContext::new(
                symbol.clone(),
                time,
                TimeSpan::ONE_MINUTE,
                funding,
                open_interest,
                prev_day_px,
                day_ntl_vlm,
                premium,
                oracle_px,
                mark_px,
                mid_px,
                impact_bid_px,
                impact_ask_px,
            );
            if let Some(quote_bar) = quote_bar_from_perpetual_context(&context) {
                download.quote_bars.push(quote_bar);
            }
            download.margin_interest_rates.push(MarginInterestRate::new(
                symbol.clone(),
                time,
                funding,
            ));
            download.open_interest_ticks.push(Tick::open_interest(
                symbol.clone(),
                time,
                open_interest,
            ));
            download.contexts.push(context);
        }
    }

    sort_and_dedupe_perpetual_contexts(&mut download.contexts);
    sort_and_dedupe_quote_bars(&mut download.quote_bars);
    sort_and_dedupe_ticks(&mut download.open_interest_ticks);
    Ok(download)
}

fn parse_decimal_str(raw: &str, field: &str) -> Result<Decimal> {
    raw.trim().parse::<Decimal>().with_context(|| {
        format!("failed to parse Hyperliquid asset_ctxs decimal field {field}={raw:?}")
    })
}

fn quote_bar_from_perpetual_context(context: &PerpetualContext) -> Option<QuoteBar> {
    if context.impact_bid_px <= Decimal::ZERO
        || context.impact_ask_px <= Decimal::ZERO
        || context.impact_ask_px < context.impact_bid_px
    {
        return None;
    }

    Some(QuoteBar::new(
        context.symbol.clone(),
        context.time,
        TimeSpan::ONE_MINUTE,
        Some(Bar::from_price(context.impact_bid_px)),
        Some(Bar::from_price(context.impact_ask_px)),
        Decimal::ZERO,
        Decimal::ZERO,
    ))
}

fn quote_bar_from_trade_bar_with_impact_ratio(
    bar: &TradeBar,
    ratio: ImpactQuoteRatio,
) -> Option<QuoteBar> {
    if ratio.bid <= Decimal::ZERO
        || ratio.ask <= Decimal::ZERO
        || ratio.ask < ratio.bid
        || !bar.is_valid()
    {
        return None;
    }

    let bid = Bar::new(
        bar.open * ratio.bid,
        bar.high * ratio.bid,
        bar.low * ratio.bid,
        bar.close * ratio.bid,
    );
    let ask = Bar::new(
        bar.open * ratio.ask,
        bar.high * ratio.ask,
        bar.low * ratio.ask,
        bar.close * ratio.ask,
    );
    Some(QuoteBar::new(
        bar.symbol.clone(),
        bar.time,
        bar.period,
        Some(bid),
        Some(ask),
        Decimal::ZERO,
        Decimal::ZERO,
    ))
}

fn custom_string_field(point: &lean_data::CustomDataPoint, field: &str) -> Option<String> {
    point
        .fields
        .get(field)?
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn custom_decimal_field(point: &lean_data::CustomDataPoint, field: &str) -> Option<Decimal> {
    parse_decimal(point.fields.get(field)?).filter(|value| *value != Decimal::ZERO)
}

fn aggregate_trade_bars(bars: &[TradeBar], resolution: Resolution) -> Result<Vec<TradeBar>> {
    if resolution == Resolution::Minute {
        return Ok(bars.to_vec());
    }
    let period = resolution
        .to_time_span()
        .with_context(|| format!("resolution {resolution:?} cannot produce trade bars"))?;
    let mut aggregates: BTreeMap<(u64, i64), TradeBar> = BTreeMap::new();

    let mut sorted = bars.to_vec();
    sorted.sort_by_key(|bar| (bar.symbol.id.sid, bar.time.0));
    for bar in sorted {
        let bucket_time = quote_bar_bucket_time(bar.time, resolution)?;
        let key = (bar.symbol.id.sid, bucket_time.0);
        match aggregates.get_mut(&key) {
            Some(existing) => {
                existing.merge(&bar);
                existing.end_time = bucket_time + period;
                existing.period = period;
            }
            None => {
                let mut aggregate = bar.clone();
                aggregate.time = bucket_time;
                aggregate.end_time = bucket_time + period;
                aggregate.period = period;
                aggregates.insert(key, aggregate);
            }
        }
    }

    let mut output = aggregates.into_values().collect::<Vec<_>>();
    sort_and_dedupe_trade_bars(&mut output);
    Ok(output)
}

fn aggregate_quote_bars(bars: &[QuoteBar], resolution: Resolution) -> Result<Vec<QuoteBar>> {
    if resolution == Resolution::Minute {
        return Ok(bars.to_vec());
    }
    let period = resolution
        .to_time_span()
        .with_context(|| format!("resolution {resolution:?} cannot produce quote bars"))?;
    let mut aggregates: BTreeMap<(u64, i64), QuoteBar> = BTreeMap::new();

    let mut sorted = bars.to_vec();
    sorted.sort_by_key(|bar| (bar.symbol.id.sid, bar.time.0));
    for bar in sorted {
        let bucket_time = quote_bar_bucket_time(bar.time, resolution)?;
        let key = (bar.symbol.id.sid, bucket_time.0);
        match aggregates.get_mut(&key) {
            Some(existing) => {
                existing.merge(&bar);
                existing.end_time = bucket_time + period;
                existing.period = period;
            }
            None => {
                let mut aggregate = bar.clone();
                aggregate.time = bucket_time;
                aggregate.end_time = bucket_time + period;
                aggregate.period = period;
                aggregates.insert(key, aggregate);
            }
        }
    }

    let mut output = aggregates.into_values().collect::<Vec<_>>();
    sort_and_dedupe_quote_bars(&mut output);
    Ok(output)
}

fn quote_bar_bucket_time(time: DateTime, resolution: Resolution) -> Result<DateTime> {
    match resolution {
        Resolution::Minute => Ok(NanosecondTimestamp(
            time.0.div_euclid(TimeSpan::ONE_MINUTE.nanos) * TimeSpan::ONE_MINUTE.nanos,
        )),
        Resolution::Hour => Ok(NanosecondTimestamp(
            time.0.div_euclid(TimeSpan::ONE_HOUR.nanos) * TimeSpan::ONE_HOUR.nanos,
        )),
        Resolution::Daily => Ok(date_to_datetime(time.date_utc(), 0, 0, 0)),
        Resolution::Tick | Resolution::Second => Err(anyhow::anyhow!(
            "NotImplemented: Hyperliquid asset_ctxs does not support {resolution:?} quote bars"
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

fn parse_l2_book_archive(text: &str, coin: &str, symbol: &Symbol) -> Result<Vec<Tick>> {
    let mut ticks = Vec::new();
    for record in parse_archive_records(text)? {
        if let Some(tick) = parse_l2_record(&record, coin, symbol)? {
            ticks.push(tick);
        }
    }
    sort_and_dedupe_ticks(&mut ticks);
    Ok(ticks)
}

fn parse_fill_archive(text: &str, coin: &str, symbol: &Symbol) -> Result<Vec<Tick>> {
    let mut symbols_by_coin = HashMap::new();
    symbols_by_coin.insert(archive_coin_key(coin), vec![symbol.clone()]);
    parse_fill_archive_for_symbols(text, &symbols_by_coin)
}

fn parse_fill_archive_for_symbols(
    text: &str,
    symbols_by_coin: &HashMap<String, Vec<Symbol>>,
) -> Result<Vec<Tick>> {
    let mut ticks = Vec::new();
    let mut seen_trades = HashSet::new();
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
            let Some(fill_coin) = fill.get("coin").and_then(Value::as_str) else {
                continue;
            };
            let Some(symbols) = symbols_by_coin.get(&archive_coin_key(fill_coin)) else {
                continue;
            };
            let Some(price) = decimal_field(fill, "px") else {
                continue;
            };
            let Some(size) = decimal_field(fill, "sz") else {
                continue;
            };
            if price <= Decimal::ZERO || size <= Decimal::ZERO {
                continue;
            }
            let Some(time) = fill
                .get("time")
                .and_then(parse_timestamp)
                .or_else(|| record.get("block_time").and_then(parse_timestamp))
            else {
                continue;
            };
            let key = fill
                .get("tid")
                .map(|tid| format!("tid:{tid}"))
                .unwrap_or_else(|| format!("{}:{}:{}:{}", time.0, fill_coin, price, size));
            for symbol in symbols {
                if !seen_trades.insert(format!("{}:{key}", symbol.id.sid)) {
                    continue;
                }
                ticks.push(Tick::trade(symbol.clone(), time, price, size));
            }
        }
    }
    sort_and_dedupe_ticks(&mut ticks);
    Ok(ticks)
}

pub(crate) fn parse_archive_records(text: &str) -> Result<Vec<Value>> {
    let mut records = Vec::new();
    let mut parse_errors = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<Value>(trimmed) {
            Ok(Value::Array(values)) => records.extend(values),
            Ok(value) => records.push(value),
            Err(error) => parse_errors.push(error),
        }
    }

    if records.is_empty() && !text.trim().is_empty() {
        match serde_json::from_str::<Value>(text.trim())? {
            Value::Array(values) => records.extend(values),
            value => records.push(value),
        }
    } else if records.is_empty() && !parse_errors.is_empty() {
        return Err(anyhow::anyhow!(
            "failed to parse archive JSON: {}",
            parse_errors.remove(0)
        ));
    }

    Ok(records)
}

fn parse_l2_record(record: &Value, coin: &str, symbol: &Symbol) -> Result<Option<Tick>> {
    let data = record
        .pointer("/raw/data")
        .or_else(|| record.get("data"))
        .unwrap_or(record);
    let Some(record_coin) = data.get("coin").and_then(Value::as_str) else {
        return Ok(None);
    };
    if !record_coin.eq_ignore_ascii_case(coin) {
        return Ok(None);
    }

    let Some(time) = data
        .get("time")
        .and_then(parse_timestamp)
        .or_else(|| record.get("time").and_then(parse_timestamp))
    else {
        return Ok(None);
    };
    let Some(levels) = data.get("levels").and_then(Value::as_array) else {
        return Ok(None);
    };
    let Some(bid_level) = levels
        .first()
        .and_then(Value::as_array)
        .and_then(|side| side.first())
    else {
        return Ok(None);
    };
    let Some(ask_level) = levels
        .get(1)
        .and_then(Value::as_array)
        .and_then(|side| side.first())
    else {
        return Ok(None);
    };

    let Some(bid) = decimal_field(bid_level, "px") else {
        return Ok(None);
    };
    let Some(ask) = decimal_field(ask_level, "px") else {
        return Ok(None);
    };
    let Some(bid_size) = decimal_field(bid_level, "sz") else {
        return Ok(None);
    };
    let Some(ask_size) = decimal_field(ask_level, "sz") else {
        return Ok(None);
    };
    if bid <= Decimal::ZERO || ask <= Decimal::ZERO || ask < bid {
        return Ok(None);
    }

    Ok(Some(Tick::quote(
        symbol.clone(),
        time,
        bid,
        ask,
        bid_size,
        ask_size,
    )))
}

fn decimal_field(value: &Value, field: &str) -> Option<Decimal> {
    parse_decimal(value.get(field)?)
}

fn parse_decimal(value: &Value) -> Option<Decimal> {
    match value {
        Value::String(raw) => raw.parse().ok(),
        Value::Number(number) => number.to_string().parse().ok(),
        _ => None,
    }
}

pub(crate) fn parse_timestamp(value: &Value) -> Option<DateTime> {
    if let Some(raw) = value.as_i64() {
        if raw.abs() > 10_000_000_000_000 {
            return Some(NanosecondTimestamp(raw));
        }
        return Some(DateTime::from_millis(raw));
    }
    let raw = value.as_str()?.trim();
    if let Ok(ms) = raw.parse::<i64>() {
        return Some(DateTime::from_millis(ms));
    }
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(raw) {
        return Some(dt.with_timezone(&Utc).into());
    }
    if let Ok(naive) = NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S%.f") {
        return Some(Utc.from_utc_datetime(&naive).into());
    }
    if let Ok(naive) = NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f") {
        return Some(Utc.from_utc_datetime(&naive).into());
    }
    None
}

fn info_interval(resolution: Resolution) -> Result<(&'static str, i64)> {
    match resolution {
        Resolution::Minute => Ok(("1m", 60_000)),
        Resolution::Hour => Ok(("1h", 3_600_000)),
        Resolution::Daily => Ok(("1d", 86_400_000)),
        Resolution::Tick | Resolution::Second => Err(anyhow::anyhow!(
            "NotImplemented: Hyperliquid Info API does not support {resolution:?} trade bars"
        )),
    }
}

fn parse_candle_snapshot(
    value: &Value,
    symbol: &Symbol,
    resolution: Resolution,
    start: DateTime,
    end: DateTime,
) -> Result<Vec<TradeBar>> {
    let period = resolution
        .to_time_span()
        .with_context(|| format!("resolution {resolution:?} cannot produce trade bars"))?;
    let Some(rows) = value.as_array() else {
        return Err(anyhow::anyhow!(
            "Hyperliquid candleSnapshot response was not an array: {value}"
        ));
    };

    let mut bars = Vec::new();
    for row in rows {
        let Some(time) = row.get("t").and_then(parse_timestamp) else {
            continue;
        };
        if time < start || time > end {
            continue;
        }
        let Some(open) = decimal_field(row, "o") else {
            continue;
        };
        let Some(high) = decimal_field(row, "h") else {
            continue;
        };
        let Some(low) = decimal_field(row, "l") else {
            continue;
        };
        let Some(close) = decimal_field(row, "c") else {
            continue;
        };
        let Some(volume) = decimal_field(row, "v") else {
            continue;
        };
        if open <= Decimal::ZERO
            || high <= Decimal::ZERO
            || low <= Decimal::ZERO
            || close <= Decimal::ZERO
        {
            continue;
        }

        bars.push(TradeBar::new(
            symbol.clone(),
            time,
            period,
            TradeBarData::new(open, high, low, close, volume),
        ));
    }
    sort_and_dedupe_trade_bars(&mut bars);
    Ok(bars)
}

fn parse_funding_history(
    value: &Value,
    symbol: &Symbol,
    start: DateTime,
    end: DateTime,
) -> Result<Vec<MarginInterestRate>> {
    let Some(rows) = value.as_array() else {
        return Err(anyhow::anyhow!(
            "Hyperliquid fundingHistory response was not an array: {value}"
        ));
    };

    let mut rates = Vec::new();
    for row in rows {
        let Some(time) = row.get("time").and_then(parse_timestamp) else {
            continue;
        };
        if time < start || time > end {
            continue;
        }
        let Some(rate) =
            decimal_field(row, "fundingRate").or_else(|| decimal_field(row, "funding"))
        else {
            continue;
        };
        rates.push(MarginInterestRate::new(symbol.clone(), time, rate));
    }
    sort_and_dedupe_margin_interest_rates(&mut rates);
    Ok(rates)
}

fn sort_and_dedupe_ticks(ticks: &mut Vec<Tick>) {
    ticks.sort_by(|a, b| {
        (
            a.symbol.id.sid,
            a.time.0,
            a.tick_type as u8,
            a.value,
            a.quantity,
            a.bid_price,
            a.ask_price,
            a.bid_size,
            a.ask_size,
        )
            .cmp(&(
                b.symbol.id.sid,
                b.time.0,
                b.tick_type as u8,
                b.value,
                b.quantity,
                b.bid_price,
                b.ask_price,
                b.bid_size,
                b.ask_size,
            ))
    });
    ticks.dedup_by(|a, b| {
        a.symbol.id.sid == b.symbol.id.sid
            && a.time == b.time
            && a.tick_type == b.tick_type
            && a.value == b.value
            && a.quantity == b.quantity
            && a.bid_price == b.bid_price
            && a.ask_price == b.ask_price
            && a.bid_size == b.bid_size
            && a.ask_size == b.ask_size
    });
}

fn sort_and_dedupe_trade_bars(bars: &mut Vec<TradeBar>) {
    bars.sort_by_key(|bar| (bar.symbol.id.sid, bar.time.0));
    bars.dedup_by_key(|bar| (bar.symbol.id.sid, bar.time.0));
}

fn sort_and_dedupe_quote_bars(bars: &mut Vec<QuoteBar>) {
    bars.sort_by_key(|bar| (bar.symbol.id.sid, bar.time.0));
    bars.dedup_by_key(|bar| (bar.symbol.id.sid, bar.time.0));
}

fn sort_and_dedupe_margin_interest_rates(rates: &mut Vec<MarginInterestRate>) {
    rates.sort_by_key(|rate| (rate.symbol.id.sid, rate.time.0));
    rates.dedup_by_key(|rate| (rate.symbol.id.sid, rate.time.0));
}

fn sort_and_dedupe_perpetual_contexts(contexts: &mut Vec<PerpetualContext>) {
    contexts.sort_by_key(|context| (context.symbol.id.sid, context.time.0));
    contexts.dedup_by_key(|context| (context.symbol.id.sid, context.time.0));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::{encode_lz4_text, ArchiveBuckets, ArchiveRegions};
    use tempfile::TempDir;

    fn provider(temp: &TempDir, coin_map: HashMap<String, String>) -> HyperliquidHistoryProvider {
        let data_root = temp.path().join("lean-data");
        let archive = S3ArchiveClient::new(
            Some(temp.path().join("archive-cache")),
            ArchiveBuckets {
                market: "hyperliquid-archive".to_string(),
                fills: "hl-mainnet-node-data".to_string(),
            },
            "requester",
            ArchiveRegions {
                market: "us-east-1".to_string(),
                fills: "ap-northeast-1".to_string(),
            },
            None,
        );
        HyperliquidHistoryProvider::new(
            &data_root,
            archive,
            HyperliquidArchiveConfig {
                coin_map,
                info_url: None,
            },
        )
    }

    fn write_cached_archive(temp: &TempDir, bucket: &str, key: &str, text: &str) {
        let path = temp.path().join("archive-cache").join(bucket).join(key);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, encode_lz4_text(text).unwrap()).unwrap();
    }

    fn request(symbol: Symbol, data_type: DataType, resolution: Resolution) -> HistoryRequest {
        HistoryRequest {
            symbol,
            resolution,
            start: DateTime::from_millis(1_761_336_000_000),
            end: DateTime::from_millis(1_761_336_060_000),
            data_type,
        }
    }

    #[tokio::test]
    async fn generates_ticks_from_cached_archives() {
        let temp = TempDir::new().unwrap();
        let provider = provider(&temp, HashMap::new());
        let symbol = Symbol::create_crypto_future("APT", &Market::hyperliquid());
        write_cached_archive(
            &temp,
            "hyperliquid-archive",
            "market_data/20251024/20/l2Book/APT.lz4",
            r#"{"time":"2025-10-24T20:00:03.000000000","raw":{"channel":"l2Book","data":{"coin":"APT","time":1761336003000,"levels":[[{"px":"3.276","sz":"1216.64","n":2}],[{"px":"3.2768","sz":"300.0","n":1}]]}}}
{"time":"2025-10-24T20:00:30.000000000","raw":{"channel":"l2Book","data":{"coin":"APT","time":1761336030000,"levels":[[{"px":"3.280","sz":"100.0","n":1}],[{"px":"3.281","sz":"200.0","n":1}]]}}}
"#,
        );
        write_cached_archive(
            &temp,
            "hl-mainnet-node-data",
            "node_fills_by_block/hourly/20251024/20.lz4",
            r#"{"block_time":"2025-10-24T20:00:00.021156698","events":[["0x1",{"coin":"APT","px":"3.277","sz":"7.0","side":"B","time":1761336000021,"tid":42}],["0x2",{"coin":"APT","px":"3.277","sz":"7.0","side":"A","time":1761336000021,"tid":42}]]}
"#,
        );
        write_cached_archive(
            &temp,
            "hyperliquid-archive",
            "asset_ctxs/20251024.csv.lz4",
            "time,coin,funding,open_interest,prev_day_px,day_ntl_vlm,premium,oracle_px,mark_px,mid_px,impact_bid_px,impact_ask_px\n\
2025-10-24 20:00:00.000,APT,0.0000125,123456.7,3.20,456789.0,0.0002,3.2765,3.2767,3.2768,3.2760,3.2770\n\
2025-10-24 20:01:00.000,BTC,0.000002,1.0,100.0,1.0,0.0,100.0,100.0,100.0,99.9,100.1\n",
        );
        let ticks = provider
            .get_ticks(&request(symbol.clone(), DataType::Tick, Resolution::Tick))
            .await
            .unwrap();
        assert_eq!(ticks.iter().filter(|tick| tick.is_trade()).count(), 1);
        assert_eq!(ticks.iter().filter(|tick| tick.is_quote()).count(), 2);

        let quote_bars = provider
            .get_quote_bars(&request(
                symbol.clone(),
                DataType::QuoteBar,
                Resolution::Minute,
            ))
            .await
            .unwrap();
        assert_eq!(quote_bars.len(), 1);
        assert_eq!(
            quote_bars[0].bid.as_ref().unwrap().close,
            Decimal::new(32760, 4)
        );
        assert_eq!(
            quote_bars[0].ask.as_ref().unwrap().close,
            Decimal::new(32770, 4)
        );

        let contexts = provider
            .get_perpetual_contexts(&request(
                symbol.clone(),
                DataType::PerpetualContext,
                Resolution::Minute,
            ))
            .await
            .unwrap();
        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].funding, Decimal::new(125, 7));
        assert_eq!(contexts[0].open_interest, Decimal::new(1_234_567, 1));
        assert_eq!(contexts[0].mark_px, Decimal::new(32_767, 4));

        let rates = provider
            .get_margin_interest_rates(&request(
                symbol.clone(),
                DataType::MarginInterestRate,
                Resolution::Hour,
            ))
            .await
            .unwrap();
        assert_eq!(rates.len(), 1);
        assert_eq!(rates[0].symbol.value, "APT");
        assert_eq!(rates[0].time, DateTime::from_millis(1_761_336_000_000));
        assert_eq!(rates[0].interest_rate, Decimal::new(125, 7));

        let cached_trade_bar = TradeBar::new(
            symbol.clone(),
            DateTime::from_millis(1_761_336_000_000),
            TimeSpan::ONE_MINUTE,
            TradeBarData::new(
                Decimal::new(3277, 3),
                Decimal::new(3277, 3),
                Decimal::new(3277, 3),
                Decimal::new(3277, 3),
                Decimal::new(70, 1),
            ),
        );
        provider
            .write_trade_bars_by_day(&symbol, Resolution::Minute, &[cached_trade_bar])
            .unwrap();

        let trade_bars = provider
            .get_history(&request(
                symbol.clone(),
                DataType::TradeBar,
                Resolution::Minute,
            ))
            .await
            .unwrap();
        assert_eq!(trade_bars.len(), 1);
        assert_eq!(trade_bars[0].open, Decimal::new(3277, 3));
        assert_eq!(trade_bars[0].close, Decimal::new(3277, 3));
        assert_eq!(trade_bars[0].volume, Decimal::new(70, 1));
    }

    #[tokio::test]
    async fn hip3_trade_requests_backfill_impact_quote_bars_from_asset_contexts() {
        let temp = TempDir::new().unwrap();
        let provider = provider(&temp, HashMap::new());
        let symbol = Symbol::create_crypto_future("XYZ:TSLA", &Market::hyperliquid());
        let cached_trade_bar = TradeBar::new(
            symbol.clone(),
            DateTime::from_millis(1_761_336_000_000),
            TimeSpan::ONE_MINUTE,
            TradeBarData::new(
                Decimal::new(25225, 2),
                Decimal::new(25225, 2),
                Decimal::new(25225, 2),
                Decimal::new(25225, 2),
                Decimal::new(20, 1),
            ),
        );
        provider
            .write_trade_bars_by_day(&symbol, Resolution::Minute, &[cached_trade_bar])
            .unwrap();
        write_cached_archive(
            &temp,
            "hyperliquid-archive",
            "asset_ctxs/20251024.csv.lz4",
            "time,coin,funding,open_interest,prev_day_px,day_ntl_vlm,premium,oracle_px,mark_px,mid_px,impact_bid_px,impact_ask_px\n\
2025-10-24 20:00:00.000,xyz:TSLA,0.000031,100.0,250.00,10000.0,0.0002,252.00,252.10,252.20,251.80,252.70\n",
        );

        let trade_bars = provider
            .get_history(&request(
                symbol.clone(),
                DataType::TradeBar,
                Resolution::Minute,
            ))
            .await
            .unwrap();
        assert_eq!(trade_bars.len(), 1);
        assert_eq!(trade_bars[0].symbol.value, "XYZ:TSLA");

        let cached_quote_bars = provider
            .read_quote_bars(
                &symbol,
                Resolution::Minute,
                DateTime::from_millis(1_761_336_000_000),
                DateTime::from_millis(1_761_336_060_000),
            )
            .unwrap();
        assert_eq!(cached_quote_bars.len(), 1);
        assert_eq!(
            cached_quote_bars[0].bid.as_ref().unwrap().close,
            Decimal::new(25180, 2)
        );
        assert_eq!(
            cached_quote_bars[0].ask.as_ref().unwrap().close,
            Decimal::new(25270, 2)
        );

        let direct_quote_bars = provider
            .get_quote_bars(&request(
                symbol.clone(),
                DataType::QuoteBar,
                Resolution::Minute,
            ))
            .await
            .unwrap();
        assert_eq!(direct_quote_bars.len(), 1);
        assert_eq!(direct_quote_bars[0].symbol.value, "XYZ:TSLA");
    }

    #[test]
    fn hip3_custom_universe_rows_materialize_market_data_partitions() {
        let temp = TempDir::new().unwrap();
        let provider = provider(&temp, HashMap::new());
        let symbol = Symbol::create_crypto_future("XYZ:TSLA", &Market::hyperliquid());
        let date = chrono::NaiveDate::from_ymd_opt(2025, 10, 24).unwrap();
        let time = DateTime::from_millis(1_761_336_000_000);
        let end = time + TimeSpan::ONE_MINUTE;
        let path = custom_data_path(
            temp.path().join("lean-data"),
            "hyperliquid",
            "HIP3_XYZ",
            date,
        );
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        provider
            .writer
            .write_custom_data_points(
                &[lean_data::CustomDataPoint {
                    time: date,
                    end_time: Some(time),
                    value: Decimal::new(25220, 2),
                    fields: HashMap::from([
                        ("source".to_string(), serde_json::json!("asset_ctxs")),
                        ("symbol".to_string(), serde_json::json!("XYZ:TSLA")),
                        ("coin".to_string(), serde_json::json!("xyz:TSLA")),
                        ("funding".to_string(), serde_json::json!("0.000031")),
                        ("open_interest".to_string(), serde_json::json!("100.0")),
                        ("prev_day_px".to_string(), serde_json::json!("250.00")),
                        ("day_ntl_vlm".to_string(), serde_json::json!("10000.0")),
                        ("premium".to_string(), serde_json::json!("0.0002")),
                        ("oracle_px".to_string(), serde_json::json!("252.00")),
                        ("mark_px".to_string(), serde_json::json!("252.10")),
                        ("mid_px".to_string(), serde_json::json!("252.20")),
                        ("impact_bid_px".to_string(), serde_json::json!("251.80")),
                        ("impact_ask_px".to_string(), serde_json::json!("252.70")),
                    ]),
                }],
                &path,
            )
            .unwrap();

        provider
            .ensure_hip3_market_data_from_custom_universe(
                std::slice::from_ref(&symbol),
                Resolution::Minute,
                time,
                end,
            )
            .unwrap();

        let trade_bars = provider
            .read_trade_bars(&symbol, Resolution::Minute, time, end)
            .unwrap();
        assert_eq!(trade_bars.len(), 1);
        assert_eq!(trade_bars[0].close, Decimal::new(25220, 2));
        assert_eq!(trade_bars[0].volume, Decimal::ZERO);

        let quote_bars = provider
            .read_quote_bars(&symbol, Resolution::Minute, time, end)
            .unwrap();
        assert_eq!(quote_bars.len(), 1);
        assert_eq!(
            quote_bars[0].bid.as_ref().unwrap().close,
            Decimal::new(25180, 2)
        );
        assert_eq!(
            quote_bars[0].ask.as_ref().unwrap().close,
            Decimal::new(25270, 2)
        );

        let rates = provider
            .read_margin_interest_rates(&symbol, time, end)
            .unwrap();
        assert_eq!(rates.len(), 1);
        assert_eq!(rates[0].interest_rate, Decimal::new(31, 6));

        let contexts = provider
            .read_perpetual_contexts(&symbol, time, end)
            .unwrap();
        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].mid_px, Decimal::new(25220, 2));
    }

    #[test]
    fn derives_quote_bar_from_trade_bar_and_impact_ratios() {
        let symbol = Symbol::create_crypto_future("XYZ:TSLA", &Market::hyperliquid());
        let bar = TradeBar::new(
            symbol.clone(),
            DateTime::from_millis(1_761_336_000_000),
            TimeSpan::ONE_HOUR,
            TradeBarData::new(
                Decimal::new(1000, 1),
                Decimal::new(1050, 1),
                Decimal::new(950, 1),
                Decimal::new(1020, 1),
                Decimal::new(25, 0),
            ),
        );
        let quote = quote_bar_from_trade_bar_with_impact_ratio(
            &bar,
            ImpactQuoteRatio {
                bid: Decimal::new(999, 3),
                ask: Decimal::new(1002, 3),
            },
        )
        .unwrap();

        assert_eq!(quote.symbol, symbol);
        assert_eq!(quote.period, TimeSpan::ONE_HOUR);
        assert_eq!(quote.bid.as_ref().unwrap().open, Decimal::new(99900, 3));
        assert_eq!(quote.ask.as_ref().unwrap().open, Decimal::new(100200, 3));
        assert_eq!(quote.bid.as_ref().unwrap().close, Decimal::new(101898, 3));
        assert_eq!(quote.ask.as_ref().unwrap().close, Decimal::new(102204, 3));
        assert!(quote.ask.as_ref().unwrap().close > quote.bid.as_ref().unwrap().close);
    }

    #[test]
    fn parses_candle_snapshot_trade_bars() {
        let symbol = Symbol::create_crypto_future("BTC", &Market::hyperliquid());
        let response = serde_json::json!([
            {
                "t": 1777420800000_i64,
                "T": 1777420859999_i64,
                "s": "BTC",
                "i": "1m",
                "o": "100.0",
                "h": "101.0",
                "l": "99.5",
                "c": "100.5",
                "v": "42.25",
                "n": 17
            }
        ]);

        let bars = parse_candle_snapshot(
            &response,
            &symbol,
            Resolution::Minute,
            DateTime::from_millis(1_777_420_800_000),
            DateTime::from_millis(1_777_420_860_000),
        )
        .unwrap();

        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0].open.to_string(), "100.0");
        assert_eq!(bars[0].high.to_string(), "101.0");
        assert_eq!(bars[0].low.to_string(), "99.5");
        assert_eq!(bars[0].close.to_string(), "100.5");
        assert_eq!(bars[0].volume.to_string(), "42.25");
    }

    #[tokio::test]
    async fn resolves_crypto_spot_archive_coin_from_config() {
        let temp = TempDir::new().unwrap();
        let mut coin_map = HashMap::new();
        coin_map.insert("UBTCUSDC".to_string(), "@142".to_string());
        let provider = provider(&temp, coin_map);
        let symbol = Symbol::create_crypto("UBTCUSDC", &Market::hyperliquid());

        write_cached_archive(
            &temp,
            "hyperliquid-archive",
            "market_data/20251024/20/l2Book/@142.lz4",
            r#"{"raw":{"data":{"coin":"@142","time":1761336003000,"levels":[[{"px":"10","sz":"1"}],[{"px":"10.1","sz":"2"}]]}}}
"#,
        );
        write_cached_archive(
            &temp,
            "hl-mainnet-node-data",
            "node_fills_by_block/hourly/20251024/20.lz4",
            r#"{"events":[["0x1",{"coin":"@142","px":"10.05","sz":"3","time":1761336000021,"tid":"spot-1"}]]}
"#,
        );

        let ticks = provider
            .get_ticks(&request(symbol, DataType::Tick, Resolution::Tick))
            .await
            .unwrap();
        assert_eq!(ticks.iter().filter(|tick| tick.is_trade()).count(), 1);
        assert_eq!(ticks.iter().filter(|tick| tick.is_quote()).count(), 1);
    }

    #[test]
    fn resolves_usa500usd_to_hip3_sp500_archive_coin_alias() {
        let temp = TempDir::new().unwrap();
        let provider = provider(&temp, HashMap::new());
        let symbol = Symbol::create_crypto_future("USA500USD", &Market::hyperliquid());

        assert_eq!(provider.archive_coin(&symbol).unwrap(), "xyz:SP500");
    }

    #[test]
    fn resolves_hip3_archive_coin_with_lowercase_dex_prefix() {
        let temp = TempDir::new().unwrap();
        let provider = provider(&temp, HashMap::new());
        let symbol = Symbol::create_crypto_future("XYZ:KPEPE", &Market::hyperliquid());

        assert_eq!(provider.archive_coin(&symbol).unwrap(), "xyz:kPEPE");

        let sp500 = Symbol::create_crypto_future("XYZ:USA500USD", &Market::hyperliquid());
        assert_eq!(provider.archive_coin(&sp500).unwrap(), "xyz:SP500");
    }

    #[test]
    fn parses_funding_history_margin_interest_rates() {
        let symbol = Symbol::create_crypto_future("XYZ:TSLA", &Market::hyperliquid());
        let response = serde_json::json!([
            { "coin": "xyz:TSLA", "fundingRate": "-0.0000125", "premium": "0.0", "time": 1764547200000_i64 },
            { "coin": "xyz:TSLA", "fundingRate": "0.000003", "premium": "0.0", "time": 1764550800000_i64 }
        ]);

        let rates = parse_funding_history(
            &response,
            &symbol,
            DateTime::from_millis(1_764_547_200_000),
            DateTime::from_millis(1_764_550_800_000),
        )
        .unwrap();

        assert_eq!(rates.len(), 2);
        assert_eq!(rates[0].symbol.value, "XYZ:TSLA");
        assert_eq!(rates[0].interest_rate, Decimal::new(-125, 7));
        assert_eq!(rates[1].interest_rate, Decimal::new(3, 6));
    }
}
