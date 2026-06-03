use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{NaiveDate, NaiveDateTime, TimeZone, Timelike, Utc};
use lean_core::{
    DateTime, Market, NanosecondTimestamp, Resolution, SecurityType, Symbol, TickType,
};
use lean_data::{MarginInterestRate, QuoteBar, Tick, TradeBar, TradeBarData};
use lean_data_providers::{DataType, HistoryRequest, IHistoryProvider};
use lean_storage::{ParquetReader, ParquetWriter, PathResolver, QueryParams, WriterConfig};
use rust_decimal::Decimal;
use serde_json::json;
use serde_json::Value;
use tracing::info;

use crate::archive::{ArchiveRuntime, S3ArchiveClient};

const HOUR_NANOS: i64 = 3_600_000_000_000;
const DEFAULT_INFO_URL: &str = "https://api.hyperliquid.xyz/info";
const MAX_CANDLES_PER_REQUEST: i64 = 5_000;
const FUNDING_CHUNK_HOURS: i64 = 500;
const INFO_API_MIN_REQUEST_INTERVAL: Duration = Duration::from_millis(500);
const INFO_API_MAX_RETRIES: usize = 6;
const INFO_API_RETRY_BASE_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Default)]
pub struct HyperliquidArchiveConfig {
    pub coin_map: HashMap<String, String>,
    pub info_url: Option<String>,
}

#[derive(Clone)]
struct HyperliquidInfoClient {
    endpoint: String,
    runtime: Arc<ArchiveRuntime>,
    next_request_at: Arc<Mutex<Instant>>,
}

impl HyperliquidInfoClient {
    fn new(endpoint: impl Into<String>) -> Self {
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

    fn candle_snapshot(
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

    fn funding_history(&self, coin: &str, start_ms: i64, end_ms: i64) -> Result<Value> {
        self.post(json!({
            "type": "fundingHistory",
            "coin": coin,
            "startTime": start_ms,
            "endTime": end_ms,
        }))
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

    async fn ensure_margin_interest_rates(
        &self,
        symbol: &Symbol,
        start: DateTime,
        end: DateTime,
    ) -> Result<()> {
        let coin = self.archive_coin(symbol)?;
        let mut rates = Vec::new();
        let mut current = start.as_millis();
        let end_ms = end.as_millis();
        while current <= end_ms {
            let chunk_end = end_ms.min(current + FUNDING_CHUNK_HOURS * 3_600_000 - 1);
            let response = self.info.funding_history(&coin, current, chunk_end)?;
            rates.extend(parse_funding_history(&response, &coin, symbol, start, end)?);
            current = chunk_end + 1;
        }
        self.write_margin_interest_rates_by_day(&rates)?;
        Ok(())
    }

    fn archive_coin(&self, symbol: &Symbol) -> Result<String> {
        validate_hyperliquid_symbol(symbol)?;
        let key = symbol.value.trim().to_ascii_uppercase();
        if let Some(mapped) = self.config.coin_map.get(&key) {
            return Ok(mapped.clone());
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
            .fetch_trade_bars_from_info(
                &request.symbol,
                request.resolution,
                request.start,
                request.end,
            )
            .await?;
        self.write_trade_bars_by_day(&request.symbol, request.resolution, &bars)?;
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

        Err(anyhow::anyhow!(
            "NotImplemented: Hyperliquid Info API does not provide historical quote bars. Use trade bars for candleSnapshot history or tick resolution for S3 l2Book quote data."
        ))
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

        self.ensure_margin_interest_rates(&request.symbol, request.start, request.end)
            .await?;
        self.read_margin_interest_rates(&request.symbol, request.start, request.end)
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
        // Hyperliquid HIP-3 USA500USD is archived under the SPX coin name.
        "USA500" | "USA500USD" | "USA500USDC" | "USA500USDT" => Some("SPX"),
        _ => None,
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

fn fills_key(hour: chrono::DateTime<Utc>) -> String {
    format!(
        "node_fills_by_block/hourly/{}/{}.lz4",
        hour.format("%Y%m%d"),
        hour.hour()
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
            if !fill_coin.eq_ignore_ascii_case(coin) {
                continue;
            }
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
            if !seen_trades.insert(key) {
                continue;
            }
            ticks.push(Tick::trade(symbol.clone(), time, price, size));
        }
    }
    sort_and_dedupe_ticks(&mut ticks);
    Ok(ticks)
}

fn parse_archive_records(text: &str) -> Result<Vec<Value>> {
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

fn parse_timestamp(value: &Value) -> Option<DateTime> {
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
    coin: &str,
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
        if let Some(row_coin) = row.get("coin").and_then(Value::as_str) {
            if !row_coin.eq_ignore_ascii_case(coin) {
                continue;
            }
        }
        let Some(raw_time) = row.get("time").and_then(parse_timestamp) else {
            continue;
        };
        let time = NanosecondTimestamp(raw_time.0.div_euclid(HOUR_NANOS) * HOUR_NANOS);
        if time < start || time > end {
            continue;
        }
        let Some(rate) = row
            .get("fundingRate")
            .and_then(parse_decimal)
            .or_else(|| decimal_field(row, "funding_rate"))
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

fn sort_and_dedupe_margin_interest_rates(rates: &mut Vec<MarginInterestRate>) {
    rates.sort_by_key(|rate| (rate.symbol.id.sid, rate.time.0));
    rates.dedup_by_key(|rate| (rate.symbol.id.sid, rate.time.0));
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
            .unwrap_err();
        assert!(quote_bars.to_string().contains("NotImplemented"));
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

    #[test]
    fn parses_funding_history_and_normalizes_to_hour() {
        let symbol = Symbol::create_crypto_future("USA500USD", &Market::hyperliquid());
        let response = serde_json::json!([
            {
                "coin": "SPX",
                "fundingRate": "0.0000125",
                "premium": "0.0001",
                "time": 1777420800003_i64
            },
            {
                "coin": "BTC",
                "fundingRate": "0.000002",
                "premium": "0.0001",
                "time": 1777420800003_i64
            }
        ]);

        let rates = parse_funding_history(
            &response,
            "SPX",
            &symbol,
            DateTime::from_millis(1_777_420_800_000),
            DateTime::from_millis(1_777_424_400_000),
        )
        .unwrap();

        assert_eq!(rates.len(), 1);
        assert_eq!(rates[0].symbol.value, "USA500USD");
        assert_eq!(rates[0].time, DateTime::from_millis(1_777_420_800_000));
        assert_eq!(rates[0].interest_rate.to_string(), "0.0000125");
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
    fn resolves_usa500usd_to_spx_archive_coin_alias() {
        let temp = TempDir::new().unwrap();
        let provider = provider(&temp, HashMap::new());
        let symbol = Symbol::create_crypto_future("USA500USD", &Market::hyperliquid());

        assert_eq!(provider.archive_coin(&symbol).unwrap(), "SPX");
    }
}
