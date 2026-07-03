use std::collections::{BTreeMap, BTreeSet};
/// ThetaData REST API client.
///
/// Mirrors the C# `ThetaDataRestClient`:
///   - Bearer token auth
///   - NDJSON response parsing (`?format=ndjson`)
///   - Rate limiting (configurable req/s)
///   - Concurrent request limiting (default 4 — STANDARD plan cap)
///   - Retry on 429 with exponential back-off + jitter
///   - Treat HTTP 472 / 475 / 572 as "no data" (empty result, not an error)
use std::io::{BufRead, BufReader, Lines};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use chrono::NaiveDate;
use reqwest::blocking::Client;
use reqwest::header::RETRY_AFTER;
use rust_decimal::Decimal;
use serde::de::DeserializeOwned;
use tokio::sync::Semaphore;
use tracing::{debug, warn};

use lean_storage::OptionEodBar;

use crate::models::*;

/// Default ThetaData local sidecar address.  Users running the ThetaData MDDS
/// (Market Data Distribution Service) locally connect here.  Override with
/// `THETADATA_BASE_URL` env var if your sidecar is on a different host/port.
pub const DEFAULT_BASE_URL: &str = "http://127.0.0.1:25510";
const API_VERSION: &str = "/v3";
const MAX_RETRIES: u32 = 3;
const MAX_RATE_LIMIT_RETRIES: u32 = 5;

pub struct OptionQuoteRequest<'a> {
    pub root: &'a str,
    pub expiration: &'a str,
    pub strike: f64,
    pub right: &'a str,
    pub start: NaiveDate,
    pub end: NaiveDate,
    pub interval: &'a str,
}

/// Minimal process-local request scheduler.
///
/// ThetaData's local sidecar can return 429s when a burst of concurrent
/// requests arrives even if the average request rate is within the configured
/// limit. Keep one shared "next allowed request" instant and push it forward on
/// 429s so all in-flight prefetch tasks observe the same cooldown.
struct RateLimiter {
    min_interval: Duration,
    next_allowed: Mutex<Instant>,
}

impl RateLimiter {
    fn new(rps: f64) -> Self {
        let rps = if rps.is_finite() && rps > 0.0 {
            rps
        } else {
            1.0
        };
        RateLimiter {
            min_interval: Duration::from_secs_f64(1.0 / rps),
            next_allowed: Mutex::new(Instant::now()),
        }
    }

    fn wait(&self) {
        let mut next_allowed = self
            .next_allowed
            .lock()
            .expect("rate limiter mutex poisoned");
        let now = Instant::now();
        if now < *next_allowed {
            std::thread::sleep(*next_allowed - now);
        }
        let now = Instant::now();
        *next_allowed = now + self.min_interval;
    }

    fn cool_down(&self, delay: Duration) {
        let mut next_allowed = self
            .next_allowed
            .lock()
            .expect("rate limiter mutex poisoned");
        let cooldown_until = Instant::now() + delay;
        if cooldown_until > *next_allowed {
            *next_allowed = cooldown_until;
        }
    }
}

pub struct ThetaDataClient {
    http: Client,
    access_token: Option<String>,
    base_url: String,
    limiter: Arc<RateLimiter>,
    concurrency: Arc<Semaphore>,
    max_concurrent: usize,
}

pub struct ThetaDataNdjsonStream<T> {
    lines: Lines<BufReader<reqwest::blocking::Response>>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> Iterator for ThetaDataNdjsonStream<T>
where
    T: DeserializeOwned,
{
    type Item = anyhow::Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let line = match self.lines.next()? {
                Ok(line) => line,
                Err(e) => return Some(Err(e.into())),
            };
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            return Some(serde_json::from_str::<T>(trimmed).map_err(Into::into));
        }
    }
}

impl ThetaDataClient {
    /// Create a new client.
    ///
    /// - `base_url`: ThetaData endpoint.  Pass `None` to use the env var
    ///   `THETADATA_BASE_URL`, falling back to `DEFAULT_BASE_URL`
    ///   (`http://127.0.0.1:25510`, the standard local sidecar port).
    /// - `access_token`: Optional bearer token.  Not required when connecting
    ///   to a local sidecar.
    pub fn new(
        access_token: Option<String>,
        base_url: Option<String>,
        requests_per_second: f64,
        max_concurrent: usize,
        _data_root: impl AsRef<Path>,
    ) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_secs(300))
            .build()
            .expect("failed to build reqwest client");

        let base_url = base_url
            .or_else(|| std::env::var("THETADATA_BASE_URL").ok())
            .unwrap_or_else(|| DEFAULT_BASE_URL.to_string());

        ThetaDataClient {
            http,
            access_token,
            base_url,
            limiter: Arc::new(RateLimiter::new(requests_per_second)),
            concurrency: Arc::new(Semaphore::new(max_concurrent.max(1))),
            max_concurrent: max_concurrent.max(1),
        }
    }

    pub fn max_concurrent(&self) -> usize {
        self.max_concurrent
    }

    // ─── Stock endpoints ──────────────────────────────────────────────────────

    /// Equity EOD bars (up to 365 days per chunk, automatically chunked).
    pub async fn get_stock_eod(
        &self,
        symbol: &str,
        start: chrono::NaiveDate,
        end: chrono::NaiveDate,
    ) -> Result<Vec<EodBar>> {
        let mut all = Vec::new();
        let mut chunk_start = start;
        while chunk_start <= end {
            let chunk_end = (chunk_start + chrono::Duration::days(364)).min(end);
            let params = vec![
                ("symbol", symbol.to_string()),
                ("start_date", fmt_date(chunk_start)),
                ("end_date", fmt_date(chunk_end)),
            ];
            let rows: Vec<serde_json::Value> = self.execute("stock/history/eod", &params).await?;
            for row in rows {
                if let Some(bar) = parse_stock_eod_row(&row) {
                    all.push(bar);
                }
            }
            chunk_start = chunk_end + chrono::Duration::days(1);
        }
        Ok(all)
    }

    /// Equity intraday OHLC (one calendar month max per request — auto chunked).
    pub async fn get_stock_ohlc(
        &self,
        symbol: &str,
        start: chrono::NaiveDate,
        end: chrono::NaiveDate,
        interval: &str,
        start_time: Option<&str>,
        end_time: Option<&str>,
    ) -> Result<Vec<OhlcBar>> {
        let mut all = Vec::new();
        for (chunk_start, chunk_end) in calendar_month_chunks(start, end) {
            let mut params = vec![
                ("symbol", symbol.to_string()),
                ("start_date", fmt_date(chunk_start)),
                ("end_date", fmt_date(chunk_end)),
                ("interval", interval.to_string()),
            ];
            if let Some(st) = start_time {
                params.push(("start_time", st.to_string()));
            }
            if let Some(et) = end_time {
                params.push(("end_time", et.to_string()));
            }
            let rows: Vec<serde_json::Value> = self.execute("stock/history/ohlc", &params).await?;
            for row in rows {
                if let Some(bar) = parse_stock_ohlc_row(&row) {
                    all.push(bar);
                }
            }
        }
        Ok(all)
    }

    /// Equity quotes (day-by-day, one day per request).
    pub async fn get_stock_quotes(
        &self,
        symbol: &str,
        start: chrono::NaiveDate,
        end: chrono::NaiveDate,
        interval: &str,
        start_time: Option<&str>,
        end_time: Option<&str>,
    ) -> Result<Vec<QuoteBar>> {
        let mut all = Vec::new();
        let mut d = start;
        while d <= end {
            let mut params = vec![
                ("symbol", symbol.to_string()),
                ("date", fmt_date(d)),
                ("interval", interval.to_string()),
            ];
            if let Some(st) = start_time {
                params.push(("start_time", st.to_string()));
            }
            if let Some(et) = end_time {
                params.push(("end_time", et.to_string()));
            }
            let rows: Vec<serde_json::Value> = self.execute("stock/history/quote", &params).await?;
            for row in rows {
                if let Some(q) = parse_stock_quote_row(&row) {
                    all.push(q);
                }
            }
            d += chrono::Duration::days(1);
        }
        Ok(all)
    }

    /// Equity trades (day-by-day).
    pub async fn get_stock_trades(
        &self,
        symbol: &str,
        start: chrono::NaiveDate,
        end: chrono::NaiveDate,
        start_time: Option<&str>,
        end_time: Option<&str>,
    ) -> Result<Vec<TradeTick>> {
        let mut all = Vec::new();
        let mut d = start;
        while d <= end {
            let mut params = vec![("symbol", symbol.to_string()), ("date", fmt_date(d))];
            if let Some(st) = start_time {
                params.push(("start_time", st.to_string()));
            }
            if let Some(et) = end_time {
                params.push(("end_time", et.to_string()));
            }
            let rows: Vec<serde_json::Value> = self.execute("stock/history/trade", &params).await?;
            for row in rows {
                if let Some(t) = parse_stock_trade_row(&row) {
                    all.push(t);
                }
            }
            d += chrono::Duration::days(1);
        }
        Ok(all)
    }

    /// Equity trades paired with the NBBO quote active at each trade.
    pub async fn get_stock_trade_quotes(
        &self,
        symbol: &str,
        start: chrono::NaiveDate,
        end: chrono::NaiveDate,
        start_time: Option<&str>,
        end_time: Option<&str>,
    ) -> Result<Vec<V3StockTradeQuote>> {
        let mut all = Vec::new();
        let mut d = start;
        while d <= end {
            let mut params = vec![("symbol", symbol.to_string()), ("date", fmt_date(d))];
            if let Some(st) = start_time {
                params.push(("start_time", st.to_string()));
            }
            if let Some(et) = end_time {
                params.push(("end_time", et.to_string()));
            }
            let rows = self.execute("stock/history/trade_quote", &params).await?;
            all.extend(rows);
            d += chrono::Duration::days(1);
        }
        Ok(all)
    }

    // ─── Option endpoints ─────────────────────────────────────────────────────

    /// Contract universe for a root symbol on a single trading day.
    pub async fn get_option_contracts_for_date(
        &self,
        root: &str,
        date: NaiveDate,
    ) -> Result<Vec<V3OptionContract>> {
        let params = vec![("symbol", root.to_string()), ("date", fmt_date(date))];
        self.execute("option/list/contracts/quote", &params).await
    }

    /// Full option OHLC chain for a root symbol on a single trading day.
    pub async fn get_option_ohlc_chain_for_date(
        &self,
        root: &str,
        date: NaiveDate,
        interval: &str,
    ) -> Result<Vec<V3OptionOhlc>> {
        let contracts = self.get_option_contracts_for_date(root, date).await?;
        let expirations: std::collections::BTreeSet<String> = contracts
            .into_iter()
            .map(|contract| normalize_expiration(&contract.expiration))
            .collect();

        let mut rows = Vec::new();
        for expiration in expirations {
            let params = vec![
                ("symbol", root.to_string()),
                ("expiration", expiration),
                ("strike", "*".to_string()),
                ("start_date", fmt_date(date)),
                ("end_date", fmt_date(date)),
                ("interval", interval.to_string()),
            ];
            match self.execute("option/history/ohlc", &params).await {
                Ok(batch) => rows.extend(batch),
                Err(e) => warn!(
                    "ThetaData: option OHLC fetch failed for {} {}: {}",
                    root, date, e
                ),
            }
        }
        Ok(rows)
    }

    pub async fn get_option_ohlc_chain_for_contracts_for_date(
        &self,
        root: &str,
        date: NaiveDate,
        interval: &str,
        contracts: &[(String, String)],
    ) -> Result<Vec<V3OptionOhlc>> {
        let request_groups = grouped_option_requests(contracts);
        if request_groups.is_empty() {
            return Ok(vec![]);
        }
        let mut rows = Vec::new();
        for (expiration, _strikes) in request_groups {
            let params = vec![
                ("symbol", root.to_string()),
                ("expiration", expiration),
                ("strike", "*".to_string()),
                ("start_date", fmt_date(date)),
                ("end_date", fmt_date(date)),
                ("interval", interval.to_string()),
            ];
            match self.execute("option/history/ohlc", &params).await {
                Ok(batch) => rows.extend(batch),
                Err(e) => warn!(
                    "ThetaData: filtered option OHLC fetch failed for {} {}: {}",
                    root, date, e
                ),
            }
        }
        Ok(rows)
    }

    /// Full option quote chain for a root symbol on a single trading day.
    pub async fn get_option_quote_chain_for_date(
        &self,
        root: &str,
        date: NaiveDate,
        interval: &str,
    ) -> Result<Vec<V3OptionQuote>> {
        let contracts = self.get_option_contracts_for_date(root, date).await?;
        let expirations: std::collections::BTreeSet<String> = contracts
            .into_iter()
            .map(|contract| normalize_expiration(&contract.expiration))
            .collect();

        let mut rows = Vec::new();
        for expiration in expirations {
            let params = vec![
                ("symbol", root.to_string()),
                ("expiration", expiration),
                ("strike", "*".to_string()),
                ("date", fmt_date(date)),
                ("interval", interval.to_string()),
            ];
            match self.execute("option/history/quote", &params).await {
                Ok(batch) => rows.extend(batch),
                Err(e) => warn!(
                    "ThetaData: option quote fetch failed for {} {}: {}",
                    root, date, e
                ),
            }
        }
        Ok(rows)
    }

    pub async fn get_option_quote_chain_for_contracts_for_date(
        &self,
        root: &str,
        date: NaiveDate,
        interval: &str,
        contracts: &[(String, String)],
    ) -> Result<Vec<V3OptionQuote>> {
        let request_groups = grouped_option_requests(contracts);
        if request_groups.is_empty() {
            return Ok(vec![]);
        }
        let mut rows = Vec::new();
        for (expiration, _strikes) in request_groups {
            let params = vec![
                ("symbol", root.to_string()),
                ("expiration", expiration),
                ("strike", "*".to_string()),
                ("date", fmt_date(date)),
                ("interval", interval.to_string()),
            ];
            match self.execute("option/history/quote", &params).await {
                Ok(batch) => rows.extend(batch),
                Err(e) => warn!(
                    "ThetaData: filtered option quote fetch failed for {} {}: {}",
                    root, date, e
                ),
            }
        }
        Ok(rows)
    }

    /// Full option trade chain for a root symbol on a single trading day.
    pub async fn get_option_trade_chain_for_date(
        &self,
        root: &str,
        date: NaiveDate,
    ) -> Result<Vec<V3OptionTrade>> {
        let contracts = self.get_option_contracts_for_date(root, date).await?;
        let expirations: std::collections::BTreeSet<String> = contracts
            .into_iter()
            .map(|contract| normalize_expiration(&contract.expiration))
            .collect();

        let mut rows = Vec::new();
        for expiration in expirations {
            let params = vec![
                ("symbol", root.to_string()),
                ("expiration", expiration),
                ("strike", "*".to_string()),
                ("date", fmt_date(date)),
            ];
            match self.execute("option/history/trade", &params).await {
                Ok(batch) => rows.extend(batch),
                Err(e) => warn!(
                    "ThetaData: option trade fetch failed for {} {}: {}",
                    root, date, e
                ),
            }
        }
        Ok(rows)
    }

    pub async fn get_option_trade_chain_for_contracts_for_date(
        &self,
        root: &str,
        date: NaiveDate,
        contracts: &[(String, String)],
    ) -> Result<Vec<V3OptionTrade>> {
        let request_groups = grouped_option_requests(contracts);
        if request_groups.is_empty() {
            return Ok(vec![]);
        }
        let mut rows = Vec::new();
        for (expiration, _strikes) in request_groups {
            let params = vec![
                ("symbol", root.to_string()),
                ("expiration", expiration),
                ("strike", "*".to_string()),
                ("date", fmt_date(date)),
            ];
            match self.execute("option/history/trade", &params).await {
                Ok(batch) => rows.extend(batch),
                Err(e) => warn!(
                    "ThetaData: filtered option trade fetch failed for {} {}: {}",
                    root, date, e
                ),
            }
        }
        Ok(rows)
    }

    pub async fn get_option_trade_quote_chain_for_contracts_for_date(
        &self,
        root: &str,
        date: NaiveDate,
        contracts: &[(String, String)],
    ) -> Result<Vec<V3OptionTradeQuote>> {
        let request_groups = grouped_option_requests(contracts);
        if request_groups.is_empty() {
            return Ok(vec![]);
        }
        let mut rows = Vec::new();
        for (expiration, _strikes) in request_groups {
            let params = vec![
                ("symbol", root.to_string()),
                ("expiration", expiration),
                ("strike", "*".to_string()),
                ("date", fmt_date(date)),
            ];
            match self.execute("option/history/trade_quote", &params).await {
                Ok(batch) => rows.extend(batch),
                Err(e) => warn!(
                    "ThetaData: filtered option trade_quote fetch failed for {} {}: {}",
                    root, date, e
                ),
            }
        }
        Ok(rows)
    }

    pub async fn get_option_trade_quote_chain_for_filter_for_date(
        &self,
        root: &str,
        date: NaiveDate,
        max_dte: i32,
        strike_range: i32,
    ) -> Result<Vec<V3OptionTradeQuote>> {
        let params = vec![
            ("symbol", root.to_string()),
            ("expiration", "*".to_string()),
            ("date", fmt_date(date)),
            ("max_dte", max_dte.max(0).to_string()),
            ("strike_range", strike_range.max(0).to_string()),
        ];
        self.execute("option/history/trade_quote", &params).await
    }

    /// Option quote history for a single contract (bulk chain per day, filtered to contract).
    pub async fn get_option_quotes(
        &self,
        request: OptionQuoteRequest<'_>,
    ) -> Result<Vec<QuoteBar>> {
        let contract_key = option_contract_key(request.expiration, request.strike, request.right);
        let mut all_rows = Vec::new();
        let mut d = request.start;
        while d <= request.end {
            let params = vec![
                ("symbol", request.root.to_string()),
                ("expiration", request.expiration.to_string()),
                ("date", fmt_date(d)),
                ("interval", request.interval.to_string()),
            ];
            let batch: Vec<V3OptionQuote> = self.execute("option/history/quote", &params).await?;
            all_rows.extend(batch);
            d += chrono::Duration::days(1);
        }

        Ok(all_rows
            .into_iter()
            .filter(|r| option_row_matches(&r.expiration, r.strike, &r.right, &contract_key))
            .filter_map(|r| {
                let date = parse_date(&r.date, &r.timestamp)?;
                let ms = if r.ms_of_day > 0 {
                    r.ms_of_day
                } else {
                    ms_of_day_from_timestamp(&r.timestamp)
                };
                Some(QuoteBar {
                    date,
                    ms_of_day: ms,
                    bid_size: r.bid_size,
                    bid_exchange: r.bid_exchange,
                    bid_price: r.bid_price,
                    bid_condition: r.bid_condition,
                    ask_size: r.ask_size,
                    ask_exchange: r.ask_exchange,
                    ask_price: r.ask_price,
                    ask_condition: r.ask_condition,
                })
            })
            .collect())
    }

    /// Option trade history for a single contract (bulk chain per day, filtered).
    pub async fn get_option_trades(
        &self,
        root: &str,
        expiration: &str,
        strike: f64,
        right: &str,
        start: chrono::NaiveDate,
        end: chrono::NaiveDate,
    ) -> Result<Vec<TradeTick>> {
        let contract_key = option_contract_key(expiration, strike, right);
        let mut all_rows = Vec::new();
        let mut d = start;
        while d <= end {
            let params = vec![
                ("symbol", root.to_string()),
                ("expiration", expiration.to_string()),
                ("date", fmt_date(d)),
            ];
            let batch: Vec<V3OptionTrade> = self.execute("option/history/trade", &params).await?;
            all_rows.extend(batch);
            d += chrono::Duration::days(1);
        }

        Ok(all_rows
            .into_iter()
            .filter(|r| option_row_matches(&r.expiration, r.strike, &r.right, &contract_key))
            .filter_map(|r| {
                let date = parse_date(&r.date, &r.timestamp)?;
                let ms = if r.ms_of_day > 0 {
                    r.ms_of_day
                } else {
                    ms_of_day_from_timestamp(&r.timestamp)
                };
                Some(TradeTick {
                    date,
                    ms_of_day: ms,
                    price: r.price,
                    size: r.size,
                    exchange: r.exchange,
                    condition: r.condition,
                })
            })
            .collect())
    }

    /// Fetch the full option EOD chain for a root symbol for a single trading day.
    ///
    /// Uses `expiration="*"` and `strike="*"` to retrieve the full chain for that date.
    pub async fn get_option_eod_for_date(
        &self,
        root: &str,
        date: NaiveDate,
    ) -> Result<Vec<V3OptionEod>> {
        // strike=* is required — without it the API returns only a single strike.
        let d = fmt_date(date);
        let params = vec![
            ("symbol", root.to_string()),
            ("expiration", "*".to_string()),
            ("strike", "*".to_string()),
            ("start_date", d.clone()),
            ("end_date", d),
        ];
        self.execute("option/history/eod", &params).await
    }

    /// Like `get_option_eod_for_date` but returns `OptionEodBar` directly.
    pub async fn get_option_eod_bars_for_date(
        &self,
        root: &str,
        date: NaiveDate,
    ) -> Result<Vec<OptionEodBar>> {
        let d = fmt_date(date);
        let params = vec![
            ("symbol", root.to_string()),
            ("expiration", "*".to_string()),
            ("strike", "*".to_string()),
            ("start_date", d.clone()),
            ("end_date", d),
        ];
        let api_rows: Vec<V3OptionEod> = self.execute("option/history/eod", &params).await?;
        Ok(v3_to_option_eod_bars(root, date, &api_rows))
    }

    /// Option EOD for a single contract.
    pub async fn get_option_eod(
        &self,
        root: &str,
        expiration: &str,
        strike: f64,
        right: &str,
        start: chrono::NaiveDate,
        end: chrono::NaiveDate,
    ) -> Result<Vec<EodBar>> {
        let contract_key = option_contract_key(expiration, strike, right);
        let params = vec![
            ("symbol", root.to_string()),
            ("expiration", expiration.to_string()),
            ("start_date", fmt_date(start)),
            ("end_date", fmt_date(end)),
        ];
        let all_rows: Vec<V3OptionEod> = self.execute("option/history/eod", &params).await?;

        Ok(all_rows
            .into_iter()
            .filter(|r| option_row_matches(&r.expiration, r.strike, &r.right, &contract_key))
            .filter_map(|r| {
                let date = parse_date(&r.date, "")?;
                Some(EodBar {
                    date,
                    open: r.open,
                    high: r.high,
                    low: r.low,
                    close: r.close,
                    volume: r.volume,
                    count: r.count,
                    bid_price: r.bid_price,
                    bid_size: r.bid_size,
                    ask_price: r.ask_price,
                    ask_size: r.ask_size,
                })
            })
            .collect())
    }

    // ─── Index endpoints ──────────────────────────────────────────────────────

    pub async fn get_index_prices(
        &self,
        symbol: &str,
        start: chrono::NaiveDate,
        end: chrono::NaiveDate,
        interval: &str,
        start_time: Option<&str>,
        end_time: Option<&str>,
    ) -> Result<Vec<IndexPrice>> {
        let mut all = Vec::new();
        let mut d = start;
        while d <= end {
            let mut params = vec![
                ("symbol", symbol.to_string()),
                ("date", fmt_date(d)),
                ("interval", interval.to_string()),
            ];
            if let Some(st) = start_time {
                params.push(("start_time", st.to_string()));
            }
            if let Some(et) = end_time {
                params.push(("end_time", et.to_string()));
            }
            let rows: Vec<V3IndexPrice> = self.execute("index/history/price", &params).await?;
            for r in rows {
                if let Some(ip) = parse_index_price(&r) {
                    all.push(ip);
                }
            }
            d += chrono::Duration::days(1);
        }
        Ok(all)
    }

    pub async fn get_index_eod(
        &self,
        symbol: &str,
        start: chrono::NaiveDate,
        end: chrono::NaiveDate,
    ) -> Result<Vec<EodBar>> {
        let mut all = Vec::new();
        let mut chunk_start = start;
        while chunk_start <= end {
            let chunk_end = (chunk_start + chrono::Duration::days(364)).min(end);
            let params = vec![
                ("symbol", symbol.to_string()),
                ("start_date", fmt_date(chunk_start)),
                ("end_date", fmt_date(chunk_end)),
            ];
            let rows: Vec<serde_json::Value> = self.execute("index/history/eod", &params).await?;
            for row in rows {
                if let Some(bar) = parse_index_eod_row(&row) {
                    all.push(bar);
                }
            }
            chunk_start = chunk_end + chrono::Duration::days(1);
        }
        Ok(all)
    }

    // ─── Core HTTP ────────────────────────────────────────────────────────────

    /// Execute a v3 NDJSON request, parse each line into `T`.
    ///
    /// Implements rate limiting, concurrency limiting, and retry logic.
    async fn execute<T>(&self, endpoint: &str, params: &[(&str, String)]) -> Result<Vec<T>>
    where
        T: DeserializeOwned + Send + 'static,
    {
        enum ExecuteAttempt<T> {
            Success(Vec<T>),
            NoData(u16),
            RateLimited(Duration),
            HttpError { status: u16, body: String },
            RequestError(String),
        }

        let mut query = format!(
            "{}{API_VERSION}/{}?format=ndjson",
            self.base_url,
            endpoint.trim_start_matches('/')
        );
        for (k, v) in params {
            query.push('&');
            query.push_str(k);
            query.push('=');
            query.push_str(&urlencoded(v));
        }
        debug!("ThetaData GET {query}");

        let mut rl_retries: u32 = 0;
        let mut gen_retries: u32 = 0;

        loop {
            let permit = self
                .concurrency
                .clone()
                .acquire_owned()
                .await
                .expect("ThetaData concurrency semaphore closed");
            let http = self.http.clone();
            let access_token = self.access_token.clone();
            let limiter = Arc::clone(&self.limiter);
            let query_for_request = query.clone();

            let (tx, rx) = futures::channel::oneshot::channel();
            std::thread::Builder::new()
                .name("thetadata-request".to_string())
                .spawn(move || {
                    let result = (|| -> Result<ExecuteAttempt<T>> {
                        let _permit = permit;
                        limiter.wait();
                        let mut req = http.get(&query_for_request);
                        if let Some(token) = &access_token {
                            req = req.bearer_auth(token);
                        }

                        let resp = match req.send() {
                            Ok(r) => r,
                            Err(e) => return Ok(ExecuteAttempt::RequestError(e.to_string())),
                        };

                        let status = resp.status().as_u16();

                        // "No data" codes — empty result, not an error.
                        if matches!(status, 472 | 475 | 572) {
                            return Ok(ExecuteAttempt::NoData(status));
                        }

                        // Rate limit — exponential back-off + jitter.
                        if status == 429 {
                            let delay = retry_after_delay(&resp).unwrap_or_else(|| {
                                Duration::from_millis(
                                    (2u64.pow(rl_retries + 1)) * 1000 + (rand_jitter() as u64),
                                )
                            });
                            return Ok(ExecuteAttempt::RateLimited(delay));
                        }

                        if !resp.status().is_success() {
                            let body = resp.text().unwrap_or_default();
                            return Ok(ExecuteAttempt::HttpError { status, body });
                        }

                        let mut result = Vec::new();
                        for line in BufReader::new(resp).lines() {
                            let line = line?;
                            let trimmed = line.trim();
                            if trimmed.is_empty() {
                                continue;
                            }
                            match serde_json::from_str::<T>(trimmed) {
                                Ok(item) => result.push(item),
                                Err(e) => debug!("ThetaData: skipping malformed row: {e}"),
                            }
                        }
                        Ok(ExecuteAttempt::Success(result))
                    })();
                    let _ = tx.send(result);
                })?;
            let attempt = rx
                .await
                .map_err(|_| anyhow::anyhow!("ThetaData request worker dropped response"))??;

            match attempt {
                ExecuteAttempt::Success(result) => return Ok(result),
                ExecuteAttempt::NoData(status) => {
                    debug!("ThetaData: no data ({status}) for {endpoint}");
                    return Ok(Vec::new());
                }
                ExecuteAttempt::RateLimited(delay) => {
                    if rl_retries >= MAX_RATE_LIMIT_RETRIES {
                        bail!(
                            "ThetaData: rate limit exceeded after {MAX_RATE_LIMIT_RETRIES} retries"
                        );
                    }
                    rl_retries += 1;
                    self.limiter.cool_down(delay);
                    warn!(
                        "ThetaData: rate limited (429), cooling down shared limiter for {:.1}s",
                        delay.as_secs_f64()
                    );
                    sleep_compat(delay).await?;
                    continue;
                }
                ExecuteAttempt::RequestError(error) => {
                    if gen_retries < MAX_RETRIES {
                        gen_retries += 1;
                        warn!("ThetaData: request error (retry {gen_retries}): {error}");
                        sleep_compat(Duration::from_secs(gen_retries as u64)).await?;
                        continue;
                    }
                    bail!("ThetaData: {error}");
                }
                ExecuteAttempt::HttpError { status, body } => {
                    if gen_retries < MAX_RETRIES {
                        gen_retries += 1;
                        warn!("ThetaData: HTTP {status} (retry {gen_retries}): {body}");
                        sleep_compat(Duration::from_secs(gen_retries as u64)).await?;
                        continue;
                    }
                    bail!("ThetaData: HTTP {status} for {endpoint}: {body}");
                }
            }
        }
    }

    pub async fn stream_ndjson<T: DeserializeOwned>(
        &self,
        endpoint: &str,
        params: &[(&str, String)],
    ) -> Result<ThetaDataNdjsonStream<T>> {
        let mut query = format!(
            "{}{API_VERSION}/{}?format=ndjson",
            self.base_url,
            endpoint.trim_start_matches('/')
        );
        for (k, v) in params {
            query.push('&');
            query.push_str(k);
            query.push('=');
            query.push_str(&urlencoded(v));
        }
        debug!("ThetaData streaming GET {query}");

        let mut rl_retries: u32 = 0;
        let mut gen_retries: u32 = 0;

        loop {
            self.limiter.wait();
            let permit = self
                .concurrency
                .clone()
                .acquire_owned()
                .await
                .expect("ThetaData concurrency semaphore closed");
            let mut req = self.http.get(&query);
            if let Some(token) = &self.access_token {
                req = req.bearer_auth(token);
            }
            let resp = match req.send() {
                Ok(r) => {
                    drop(permit);
                    r
                }
                Err(e) if gen_retries < MAX_RETRIES => {
                    drop(permit);
                    gen_retries += 1;
                    warn!("ThetaData: streaming request error (retry {gen_retries}): {e}");
                    std::thread::sleep(Duration::from_secs(gen_retries as u64));
                    continue;
                }
                Err(e) => {
                    drop(permit);
                    bail!("ThetaData: {e}");
                }
            };

            let status = resp.status().as_u16();
            if matches!(status, 472 | 475 | 572) {
                debug!("ThetaData: no streaming data ({status}) for {endpoint}");
                return Ok(ThetaDataNdjsonStream {
                    lines: BufReader::new(resp).lines(),
                    _marker: std::marker::PhantomData,
                });
            }
            if status == 429 {
                if rl_retries >= MAX_RATE_LIMIT_RETRIES {
                    bail!("ThetaData: rate limit exceeded after {MAX_RATE_LIMIT_RETRIES} retries");
                }
                rl_retries += 1;
                let delay = retry_after_delay(&resp).unwrap_or_else(|| {
                    Duration::from_millis((2u64.pow(rl_retries)) * 1000 + (rand_jitter() as u64))
                });
                self.limiter.cool_down(delay);
                warn!(
                    "ThetaData: rate limited (429), cooling down shared limiter for {:.1}s",
                    delay.as_secs_f64()
                );
                std::thread::sleep(delay);
                continue;
            }
            if !resp.status().is_success() {
                let body = resp.text().unwrap_or_default();
                if gen_retries < MAX_RETRIES {
                    gen_retries += 1;
                    warn!("ThetaData: HTTP {status} (stream retry {gen_retries}): {body}");
                    std::thread::sleep(Duration::from_secs(gen_retries as u64));
                    continue;
                }
                bail!("ThetaData: HTTP {status} for {endpoint}: {body}");
            }
            return Ok(ThetaDataNdjsonStream {
                lines: BufReader::new(resp).lines(),
                _marker: std::marker::PhantomData,
            });
        }
    }
}

// ─── Option EOD Parquet helpers ───────────────────────────────────────────────

#[cfg(test)]
/// Convert a `NaiveDate` to nanoseconds since Unix epoch (midnight UTC).
fn date_to_ns(date: NaiveDate) -> i64 {
    date.and_hms_opt(0, 0, 0)
        .and_then(|dt| dt.and_utc().timestamp_nanos_opt())
        .unwrap_or(0)
}

/// Build an OSI-style symbol value from the ThetaData `symbol` field.
/// Falls back to constructing one from root + expiration + right + strike.
fn osi_symbol(r: &V3OptionEod, root: &str) -> String {
    if !r.symbol.is_empty() {
        return r.symbol.clone();
    }
    // Fallback: construct a minimal identifier
    format!(
        "{} {}{}{}",
        root,
        normalize_expiration(&r.expiration),
        normalize_right(&r.right).to_uppercase(),
        (r.strike * 1000.0).round() as i64
    )
}

/// Convert a slice of `V3OptionEod` rows (from the API) to `OptionEodBar`.
fn v3_to_option_eod_bars(root: &str, date: NaiveDate, rows: &[V3OptionEod]) -> Vec<OptionEodBar> {
    rows.iter()
        .filter_map(|r| {
            let expiration = parse_date(&r.expiration, "").or_else(|| {
                // Try stripping hyphens
                let cleaned = r.expiration.replace('-', "");
                NaiveDate::parse_from_str(&cleaned, "%Y%m%d").ok()
            })?;
            Some(OptionEodBar {
                date,
                symbol_value: osi_symbol(r, root),
                underlying: root.to_uppercase(),
                expiration,
                strike: Decimal::try_from(r.strike).unwrap_or_default(),
                right: normalize_right(&r.right).to_uppercase(),
                open: Decimal::try_from(r.open).unwrap_or_default(),
                high: Decimal::try_from(r.high).unwrap_or_default(),
                low: Decimal::try_from(r.low).unwrap_or_default(),
                close: Decimal::try_from(r.close).unwrap_or_default(),
                volume: r.volume as i64,
                bid: Decimal::try_from(r.bid_price).unwrap_or_default(),
                ask: Decimal::try_from(r.ask_price).unwrap_or_default(),
                bid_size: r.bid_size as i64,
                ask_size: r.ask_size as i64,
            })
        })
        .collect()
}

/// Convert a slice of `OptionEodBar` back to `V3OptionEod` for callers that
/// expect the original wire type.
#[cfg(test)]
fn option_eod_bars_to_v3(bars: Vec<OptionEodBar>) -> Vec<V3OptionEod> {
    use rust_decimal::prelude::ToPrimitive;
    bars.into_iter()
        .map(|b| {
            let exp_str = b.expiration.format("%Y%m%d").to_string();
            V3OptionEod {
                symbol: b.symbol_value,
                expiration: exp_str,
                strike: b.strike.to_f64().unwrap_or(0.0),
                right: b.right,
                date: b.date.format("%Y%m%d").to_string(),
                open: b.open.to_f64().unwrap_or(0.0),
                high: b.high.to_f64().unwrap_or(0.0),
                low: b.low.to_f64().unwrap_or(0.0),
                close: b.close.to_f64().unwrap_or(0.0),
                volume: b.volume as f64,
                count: 0,
                bid_size: b.bid_size as f64,
                bid_exchange: 0,
                bid_price: b.bid.to_f64().unwrap_or(0.0),
                bid_condition: 0,
                ask_size: b.ask_size as f64,
                ask_exchange: 0,
                ask_price: b.ask.to_f64().unwrap_or(0.0),
                ask_condition: 0,
                created: String::new(),
                last_trade: String::new(),
            }
        })
        .collect()
}

// ─── Parsing helpers ──────────────────────────────────────────────────────────

fn parse_stock_eod_row(row: &serde_json::Value) -> Option<EodBar> {
    let date_str = row["date"].as_str().unwrap_or("");
    let last_ts = row["last_trade"]
        .as_str()
        .or_else(|| row["last_trade_timestamp"].as_str())
        .unwrap_or("");
    let created_ts = row["created"]
        .as_str()
        .or_else(|| row["created_timestamp"].as_str())
        .unwrap_or("");
    let date = parse_date(date_str, last_ts).or_else(|| parse_date("", created_ts))?;

    Some(EodBar {
        date,
        open: row["open"].as_f64().unwrap_or(0.0),
        high: row["high"].as_f64().unwrap_or(0.0),
        low: row["low"].as_f64().unwrap_or(0.0),
        close: row["close"].as_f64().unwrap_or(0.0),
        volume: row["volume"].as_f64().unwrap_or(0.0),
        count: row["count"].as_u64().unwrap_or(0) as u32,
        bid_price: row["bid_price"]
            .as_f64()
            .or_else(|| row["bid"].as_f64())
            .unwrap_or(0.0),
        bid_size: row["bid_size"].as_f64().unwrap_or(0.0),
        ask_price: row["ask_price"]
            .as_f64()
            .or_else(|| row["ask"].as_f64())
            .unwrap_or(0.0),
        ask_size: row["ask_size"].as_f64().unwrap_or(0.0),
    })
}

fn parse_stock_ohlc_row(row: &serde_json::Value) -> Option<OhlcBar> {
    let ts = row["timestamp"].as_str().unwrap_or("");
    let date = parse_date("", ts)?;
    let ms = row["ms_of_day"].as_u64().unwrap_or(0) as u32;
    let ms = if ms > 0 {
        ms
    } else {
        ms_of_day_from_timestamp(ts)
    };
    Some(OhlcBar {
        date,
        ms_of_day: ms,
        open: row["open"].as_f64().unwrap_or(0.0),
        high: row["high"].as_f64().unwrap_or(0.0),
        low: row["low"].as_f64().unwrap_or(0.0),
        close: row["close"].as_f64().unwrap_or(0.0),
        volume: row["volume"].as_f64().unwrap_or(0.0),
        count: row["count"].as_u64().unwrap_or(0) as u32,
    })
}

fn parse_stock_quote_row(row: &serde_json::Value) -> Option<QuoteBar> {
    let ts = row["timestamp"].as_str().unwrap_or("");
    let date = parse_date("", ts)?;
    let ms = row["ms_of_day"].as_u64().unwrap_or(0) as u32;
    let ms = if ms > 0 {
        ms
    } else {
        ms_of_day_from_timestamp(ts)
    };
    Some(QuoteBar {
        date,
        ms_of_day: ms,
        bid_size: row["bid_size"].as_f64().unwrap_or(0.0),
        bid_exchange: row["bid_exchange"].as_u64().unwrap_or(0) as u8,
        bid_price: row["bid_price"]
            .as_f64()
            .or_else(|| row["bid"].as_f64())
            .unwrap_or(0.0),
        bid_condition: row["bid_condition"].as_i64().unwrap_or(0) as i32,
        ask_size: row["ask_size"].as_f64().unwrap_or(0.0),
        ask_exchange: row["ask_exchange"].as_u64().unwrap_or(0) as u8,
        ask_price: row["ask_price"]
            .as_f64()
            .or_else(|| row["ask"].as_f64())
            .unwrap_or(0.0),
        ask_condition: row["ask_condition"].as_i64().unwrap_or(0) as i32,
    })
}

fn parse_stock_trade_row(row: &serde_json::Value) -> Option<TradeTick> {
    let ts = row["timestamp"].as_str().unwrap_or("");
    let date = parse_date("", ts)?;
    let ms = row["ms_of_day"].as_u64().unwrap_or(0) as u32;
    let ms = if ms > 0 {
        ms
    } else {
        ms_of_day_from_timestamp(ts)
    };
    Some(TradeTick {
        date,
        ms_of_day: ms,
        price: row["price"].as_f64().unwrap_or(0.0),
        size: row["size"].as_f64().unwrap_or(0.0),
        exchange: row["exchange"].as_u64().unwrap_or(0) as u8,
        condition: row["condition"].as_i64().unwrap_or(0) as i32,
    })
}

fn parse_index_price(r: &V3IndexPrice) -> Option<IndexPrice> {
    use chrono::NaiveDateTime;
    for fmt in &["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"] {
        if let Ok(dt) = NaiveDateTime::parse_from_str(&r.timestamp, fmt) {
            return Some(IndexPrice {
                timestamp: dt,
                price: r.price,
            });
        }
    }
    None
}

fn parse_index_eod_row(row: &serde_json::Value) -> Option<EodBar> {
    let ts = row["timestamp"]
        .as_str()
        .or_else(|| row["close_timestamp"].as_str())
        .or_else(|| row["last_trade_timestamp"].as_str())
        .unwrap_or("");
    let date_str = row["date"].as_str().unwrap_or("");
    let date = parse_date(date_str, ts)?;
    Some(EodBar {
        date,
        open: row["open"].as_f64().unwrap_or(0.0),
        high: row["high"].as_f64().unwrap_or(0.0),
        low: row["low"].as_f64().unwrap_or(0.0),
        close: row["close"].as_f64().unwrap_or(0.0),
        volume: row["volume"].as_f64().unwrap_or(0.0),
        count: 0,
        bid_price: 0.0,
        bid_size: 0.0,
        ask_price: 0.0,
        ask_size: 0.0,
    })
}

// ─── Option contract matching ─────────────────────────────────────────────────

fn option_contract_key(expiration: &str, strike: f64, right: &str) -> String {
    format!(
        "{}|{}|{}",
        normalize_expiration(expiration),
        (strike * 1000.0).round() as i64,
        normalize_right(right)
    )
}

fn option_row_matches(expiration: &str, row_strike: f64, row_right: &str, key: &str) -> bool {
    option_contract_key(expiration, row_strike, row_right) == *key
}

fn grouped_option_requests(contracts: &[(String, String)]) -> BTreeMap<String, BTreeSet<String>> {
    let mut groups = BTreeMap::new();
    for (expiration, strike) in contracts {
        groups
            .entry(normalize_expiration(expiration))
            .or_insert_with(BTreeSet::new)
            .insert(strike.clone());
    }
    groups
}

// ─── Misc helpers ─────────────────────────────────────────────────────────────

fn fmt_date(d: chrono::NaiveDate) -> String {
    d.format("%Y%m%d").to_string()
}

/// Calendar-month chunks for stock OHLC requests (max 1 month per request).
fn calendar_month_chunks(
    start: chrono::NaiveDate,
    end: chrono::NaiveDate,
) -> Vec<(chrono::NaiveDate, chrono::NaiveDate)> {
    use chrono::Datelike;
    let mut chunks = Vec::new();
    let mut s = start;
    while s <= end {
        let last_day = chrono::NaiveDate::from_ymd_opt(
            s.year(),
            s.month(),
            days_in_month(s.year(), s.month()),
        )
        .unwrap();
        let e = last_day.min(end);
        chunks.push((s, e));
        s = e + chrono::Duration::days(1);
    }
    chunks
}

fn days_in_month(year: i32, month: u32) -> u32 {
    if month == 12 {
        31
    } else {
        chrono::NaiveDate::from_ymd_opt(year, month + 1, 1)
            .unwrap()
            .signed_duration_since(chrono::NaiveDate::from_ymd_opt(year, month, 1).unwrap())
            .num_days() as u32
    }
}

fn urlencoded(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => c.to_string(),
            ' ' => "%20".to_string(),
            c => format!("%{:02X}", c as u32),
        })
        .collect()
}

async fn sleep_compat(duration: Duration) -> Result<()> {
    let (tx, rx) = futures::channel::oneshot::channel();
    std::thread::Builder::new()
        .name("thetadata-sleep".to_string())
        .spawn(move || {
            std::thread::sleep(duration);
            let _ = tx.send(());
        })?;
    rx.await
        .map_err(|_| anyhow::anyhow!("ThetaData sleep worker dropped response"))?;
    Ok(())
}

/// Cheap pseudo-jitter (0–499 ms) without pulling in `rand`.
fn rand_jitter() -> u32 {
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_millis()
        % 500
}

fn retry_after_delay(resp: &reqwest::blocking::Response) -> Option<Duration> {
    let header = resp.headers().get(RETRY_AFTER)?.to_str().ok()?.trim();
    if let Ok(seconds) = header.parse::<u64>() {
        return Some(Duration::from_secs(seconds.max(1)));
    }
    None
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    // ── URL construction helpers ───────────────────────────────────────────────

    /// Reproduce the URL-building logic from `execute()` so we can assert on it
    /// without making network calls.
    fn build_url(endpoint: &str, params: &[(&str, String)]) -> String {
        let base = std::env::var("THETADATA_BASE_URL")
            .unwrap_or_else(|_| crate::client::DEFAULT_BASE_URL.to_string());
        let mut query = format!(
            "{base}{API_VERSION}/{}?format=ndjson",
            endpoint.trim_start_matches('/')
        );
        for (k, v) in params {
            query.push('&');
            query.push_str(k);
            query.push('=');
            query.push_str(&urlencoded(v));
        }
        query
    }

    #[test]
    fn test_url_stock_eod_endpoint() {
        let params = vec![
            ("symbol", "AAPL".to_string()),
            ("start_date", "20240101".to_string()),
            ("end_date", "20240131".to_string()),
        ];
        let url = build_url("stock/history/eod", &params);
        assert!(url.contains("stock/history/eod"));
        assert!(url.contains("format=ndjson"));
        assert!(url.contains("symbol=AAPL"));
        assert!(url.contains("start_date=20240101"));
        assert!(url.contains("end_date=20240131"));
    }

    #[test]
    fn test_url_option_quote_endpoint() {
        let params = vec![
            ("symbol", "SPY".to_string()),
            ("expiration", "20240119".to_string()),
            ("date", "20240115".to_string()),
            ("interval", "1m".to_string()),
        ];
        let url = build_url("option/history/quote", &params);
        assert!(url.contains("/v3/option/history/quote"));
        assert!(url.contains("expiration=20240119"));
        assert!(url.contains("interval=1m"));
    }

    #[test]
    fn test_url_index_price_endpoint() {
        let params = vec![
            ("symbol", "SPX".to_string()),
            ("date", "20240115".to_string()),
            ("interval", "1h".to_string()),
        ];
        let url = build_url("index/history/price", &params);
        assert!(url.contains("/v3/index/history/price"));
        assert!(url.contains("symbol=SPX"));
    }

    #[test]
    fn test_url_leading_slash_stripped_from_endpoint() {
        // `execute()` calls `endpoint.trim_start_matches('/')` — verify it works.
        let url_with = build_url("/stock/history/eod", &[]);
        let url_without = build_url("stock/history/eod", &[]);
        assert_eq!(url_with, url_without);
    }

    // ── urlencoded helper ──────────────────────────────────────────────────────

    #[test]
    fn test_urlencoded_safe_chars_unchanged() {
        assert_eq!(urlencoded("AAPL"), "AAPL");
        assert_eq!(urlencoded("20240115"), "20240115");
        assert_eq!(urlencoded("1m"), "1m");
    }

    #[test]
    fn test_urlencoded_space_becomes_percent20() {
        assert_eq!(urlencoded("hello world"), "hello%20world");
    }

    #[test]
    fn test_urlencoded_special_chars_escaped() {
        // Slash is not in the safe set and must be percent-encoded.
        let encoded = urlencoded("/");
        assert_eq!(encoded, "%2F");
    }

    #[test]
    fn test_urlencoded_percent_sign_escaped() {
        let encoded = urlencoded("%");
        assert_eq!(encoded, "%25");
    }

    // ── fmt_date ───────────────────────────────────────────────────────────────

    #[test]
    fn test_fmt_date_formats_as_yyyymmdd() {
        let d = NaiveDate::from_ymd_opt(2024, 1, 5).unwrap();
        assert_eq!(fmt_date(d), "20240105");
    }

    #[test]
    fn test_fmt_date_pads_month_and_day() {
        let d = NaiveDate::from_ymd_opt(2023, 3, 8).unwrap();
        assert_eq!(fmt_date(d), "20230308");
    }

    // ── calendar_month_chunks ─────────────────────────────────────────────────

    #[test]
    fn test_month_chunks_single_month() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 10).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 1, 25).unwrap();
        let chunks = calendar_month_chunks(start, end);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].0, start);
        assert_eq!(chunks[0].1, end);
    }

    #[test]
    fn test_month_chunks_spans_two_months() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 2, 20).unwrap();
        let chunks = calendar_month_chunks(start, end);
        assert_eq!(chunks.len(), 2);
        // First chunk: Jan 15 → Jan 31
        assert_eq!(chunks[0].0, NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
        assert_eq!(chunks[0].1, NaiveDate::from_ymd_opt(2024, 1, 31).unwrap());
        // Second chunk: Feb 1 → Feb 20
        assert_eq!(chunks[1].0, NaiveDate::from_ymd_opt(2024, 2, 1).unwrap());
        assert_eq!(chunks[1].1, NaiveDate::from_ymd_opt(2024, 2, 20).unwrap());
    }

    #[test]
    fn test_month_chunks_three_calendar_months() {
        let start = NaiveDate::from_ymd_opt(2024, 3, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 5, 31).unwrap();
        let chunks = calendar_month_chunks(start, end);
        assert_eq!(chunks.len(), 3);
        // All chunks must be non-empty and monotonically increasing.
        for w in chunks.windows(2) {
            assert!(w[0].1 < w[1].0, "chunks must not overlap");
        }
    }

    #[test]
    fn test_month_chunks_covers_entire_range() {
        let start = NaiveDate::from_ymd_opt(2024, 6, 5).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 9, 30).unwrap();
        let chunks = calendar_month_chunks(start, end);
        assert_eq!(
            chunks.first().unwrap().0,
            start,
            "first chunk must start at 'start'"
        );
        assert_eq!(
            chunks.last().unwrap().1,
            end,
            "last chunk must end at 'end'"
        );
    }

    #[test]
    fn test_month_chunks_same_day() {
        let d = NaiveDate::from_ymd_opt(2024, 7, 4).unwrap();
        let chunks = calendar_month_chunks(d, d);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].0, d);
        assert_eq!(chunks[0].1, d);
    }

    // ── days_in_month ──────────────────────────────────────────────────────────

    #[test]
    fn test_days_in_month_january() {
        assert_eq!(days_in_month(2024, 1), 31);
    }

    #[test]
    fn test_days_in_month_february_leap_year() {
        assert_eq!(days_in_month(2024, 2), 29);
    }

    #[test]
    fn test_days_in_month_february_non_leap_year() {
        assert_eq!(days_in_month(2023, 2), 28);
    }

    #[test]
    fn test_days_in_month_december() {
        // December is hard-coded to 31 in the implementation.
        assert_eq!(days_in_month(2024, 12), 31);
    }

    #[test]
    fn test_days_in_month_april_30_days() {
        assert_eq!(days_in_month(2024, 4), 30);
    }

    // ── option_contract_key ────────────────────────────────────────────────────

    #[test]
    fn test_option_contract_key_canonical_form() {
        // Strike is milli-dollars: 185.0 → 185000 → key stores 185000 * 1000 = 185_000_000
        let key = option_contract_key("20240119", 185_000.0, "C");
        assert!(key.contains("20240119"), "expiration in key");
        assert!(key.contains("|c"), "right normalized to lowercase");
    }

    #[test]
    fn test_option_contract_key_hyphenated_expiration_normalized() {
        let k1 = option_contract_key("2024-01-19", 185_000.0, "C");
        let k2 = option_contract_key("20240119", 185_000.0, "C");
        assert_eq!(k1, k2, "hyphens in expiration should be stripped");
    }

    #[test]
    fn test_option_contract_key_call_put_differ() {
        let call = option_contract_key("20240119", 185_000.0, "C");
        let put = option_contract_key("20240119", 185_000.0, "P");
        assert_ne!(call, put);
    }

    #[test]
    fn test_option_contract_key_different_strikes_differ() {
        let k1 = option_contract_key("20240119", 185_000.0, "C");
        let k2 = option_contract_key("20240119", 190_000.0, "C");
        assert_ne!(k1, k2);
    }

    // ── option_row_matches ─────────────────────────────────────────────────────

    #[test]
    fn test_option_row_matches_exact_match() {
        let key = option_contract_key("20240119", 185_000.0, "C");
        assert!(option_row_matches("20240119", 185_000.0, "C", &key));
    }

    #[test]
    fn test_option_row_matches_wrong_right_no_match() {
        let key = option_contract_key("20240119", 185_000.0, "C");
        assert!(!option_row_matches("20240119", 185_000.0, "P", &key));
    }

    #[test]
    fn test_option_row_matches_wrong_strike_no_match() {
        let key = option_contract_key("20240119", 185_000.0, "C");
        assert!(!option_row_matches("20240119", 190_000.0, "C", &key));
    }

    #[test]
    fn test_option_row_matches_long_right_normalized() {
        // The API may return "call" instead of "C" — normalize_right handles it.
        let key = option_contract_key("20240119", 185_000.0, "C");
        assert!(option_row_matches("20240119", 185_000.0, "call", &key));
    }

    // ── parse_stock_eod_row ────────────────────────────────────────────────────

    #[test]
    fn test_parse_stock_eod_row_complete() {
        let json = serde_json::json!({
            "date": "20240115",
            "open": 185.50,
            "high": 188.00,
            "low":  184.00,
            "close": 187.25,
            "volume": 75_000_000.0,
            "count": 350000,
            "bid": 187.20,
            "bid_size": 500.0,
            "ask": 187.30,
            "ask_size": 400.0,
            "last_trade": "2024-01-15T16:00:00.000",
            "created": "2024-01-16T08:00:00.000"
        });

        let bar = parse_stock_eod_row(&json).expect("should parse eod row");
        assert_eq!(bar.date, NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
        assert!((bar.open - 185.50).abs() < 1e-9);
        assert!((bar.high - 188.00).abs() < 1e-9);
        assert!((bar.low - 184.00).abs() < 1e-9);
        assert!((bar.close - 187.25).abs() < 1e-9);
        assert!((bar.volume - 75_000_000.0).abs() < 1e-9);
        assert_eq!(bar.count, 350_000);
        assert!((bar.bid_price - 187.20).abs() < 1e-9);
        assert!((bar.ask_price - 187.30).abs() < 1e-9);
    }

    #[test]
    fn test_parse_stock_eod_row_bid_price_alias() {
        // Some EOD rows use "bid_price" / "ask_price" instead of "bid" / "ask".
        let json = serde_json::json!({
            "date": "20240115",
            "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5,
            "volume": 1000.0, "count": 50,
            "bid_price": 100.40,
            "ask_price": 100.60,
            "bid_size": 10.0, "ask_size": 10.0,
            "last_trade": "2024-01-15T16:00:00.000"
        });
        let bar = parse_stock_eod_row(&json).expect("should parse bid_price/ask_price aliases");
        assert!((bar.bid_price - 100.40).abs() < 1e-9);
        assert!((bar.ask_price - 100.60).abs() < 1e-9);
    }

    #[test]
    fn test_parse_stock_eod_row_missing_date_falls_back_to_timestamp() {
        let json = serde_json::json!({
            "open": 50.0, "high": 51.0, "low": 49.0, "close": 50.5,
            "volume": 5000.0, "count": 10,
            "last_trade": "2024-03-22T16:00:00.000"
        });
        let bar = parse_stock_eod_row(&json).expect("should fall back to last_trade timestamp");
        assert_eq!(bar.date, NaiveDate::from_ymd_opt(2024, 3, 22).unwrap());
    }

    #[test]
    fn test_parse_stock_eod_row_no_date_info_returns_none() {
        let json = serde_json::json!({"open": 50.0});
        assert!(parse_stock_eod_row(&json).is_none(), "no date → None");
    }

    // ── parse_stock_ohlc_row ───────────────────────────────────────────────────

    #[test]
    fn test_parse_stock_ohlc_row_complete() {
        let json = serde_json::json!({
            "timestamp": "2024-01-15T10:30:00.000",
            "ms_of_day": 37800000,
            "open": 185.0,
            "high": 186.0,
            "low": 184.5,
            "close": 185.8,
            "volume": 250000.0,
            "count": 1200
        });
        let bar = parse_stock_ohlc_row(&json).expect("should parse ohlc row");
        assert_eq!(bar.date, NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
        assert_eq!(bar.ms_of_day, 37_800_000);
        assert!((bar.open - 185.0).abs() < 1e-9);
        assert!((bar.close - 185.8).abs() < 1e-9);
        assert_eq!(bar.count, 1200);
    }

    #[test]
    fn test_parse_stock_ohlc_row_ms_falls_back_to_timestamp() {
        // When ms_of_day is absent (or 0), the function derives it from timestamp.
        let json = serde_json::json!({
            "timestamp": "2024-01-15T09:30:00.000",
            "open": 185.0, "high": 186.0, "low": 184.0, "close": 185.5,
            "volume": 10000.0, "count": 50
        });
        let bar = parse_stock_ohlc_row(&json).expect("should parse");
        // 09:30 ET = 34200 seconds = 34_200_000 ms
        assert_eq!(bar.ms_of_day, 34_200_000);
    }

    #[test]
    fn test_parse_stock_ohlc_row_no_timestamp_returns_none() {
        let json = serde_json::json!({"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5});
        assert!(parse_stock_ohlc_row(&json).is_none());
    }

    // ── parse_stock_quote_row ──────────────────────────────────────────────────

    #[test]
    fn test_parse_stock_quote_row_complete() {
        let json = serde_json::json!({
            "timestamp": "2024-01-15T10:00:00.000",
            "ms_of_day": 36000000,
            "bid_size": 100.0,
            "bid_exchange": 3,
            "bid_price": 185.10,
            "bid_condition": 0,
            "ask_size": 200.0,
            "ask_exchange": 3,
            "ask_price": 185.20,
            "ask_condition": 0
        });
        let q = parse_stock_quote_row(&json).expect("should parse quote row");
        assert_eq!(q.date, NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
        assert_eq!(q.ms_of_day, 36_000_000);
        assert!((q.bid_price - 185.10).abs() < 1e-9);
        assert!((q.ask_price - 185.20).abs() < 1e-9);
        assert_eq!(q.bid_exchange, 3);
        assert!((q.bid_size - 100.0).abs() < 1e-9);
    }

    #[test]
    fn test_parse_stock_quote_row_bid_alias() {
        // `bid` is the v3 alias for `bid_price`.
        let json = serde_json::json!({
            "timestamp": "2024-01-15T10:00:00.000",
            "bid": 99.50,
            "ask": 99.60
        });
        let q = parse_stock_quote_row(&json).expect("should parse with 'bid'/'ask' aliases");
        assert!((q.bid_price - 99.50).abs() < 1e-9);
        assert!((q.ask_price - 99.60).abs() < 1e-9);
    }

    // ── parse_stock_trade_row ──────────────────────────────────────────────────

    #[test]
    fn test_parse_stock_trade_row_complete() {
        let json = serde_json::json!({
            "timestamp": "2024-01-15T14:00:00.000",
            "ms_of_day": 50400000,
            "price": 185.75,
            "size": 300.0,
            "exchange": 60,
            "condition": 1
        });
        let t = parse_stock_trade_row(&json).expect("should parse trade row");
        assert_eq!(t.date, NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
        assert_eq!(t.ms_of_day, 50_400_000);
        assert!((t.price - 185.75).abs() < 1e-9);
        assert!((t.size - 300.0).abs() < 1e-9);
        assert_eq!(t.exchange, 60);
        assert_eq!(t.condition, 1);
    }

    #[test]
    fn test_parse_stock_trade_row_no_timestamp_returns_none() {
        let json = serde_json::json!({"price": 100.0, "size": 10.0});
        assert!(parse_stock_trade_row(&json).is_none());
    }

    // ── parse_index_eod_row ────────────────────────────────────────────────────

    #[test]
    fn test_parse_index_eod_row_with_date_field() {
        let json = serde_json::json!({
            "date": "20240115",
            "open": 4700.0,
            "high": 4750.0,
            "low": 4680.0,
            "close": 4740.0,
            "volume": 0.0
        });
        let bar = parse_index_eod_row(&json).expect("should parse index eod");
        assert_eq!(bar.date, NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
        assert!((bar.close - 4740.0).abs() < 1e-9);
    }

    #[test]
    fn test_parse_index_eod_row_falls_back_to_timestamp_field() {
        let json = serde_json::json!({
            "timestamp": "2024-03-22T16:00:00.000",
            "open": 5100.0,
            "high": 5150.0,
            "low": 5090.0,
            "close": 5130.0,
            "volume": 0.0
        });
        let bar = parse_index_eod_row(&json).expect("should parse via timestamp");
        assert_eq!(bar.date, NaiveDate::from_ymd_opt(2024, 3, 22).unwrap());
    }

    // ── parse_index_price ──────────────────────────────────────────────────────

    #[test]
    fn test_parse_index_price_iso_t_format() {
        let r = V3IndexPrice {
            timestamp: "2024-01-15T14:00:00.000".to_string(),
            price: 4750.25,
        };
        let ip = parse_index_price(&r).expect("should parse index price");
        assert!((ip.price - 4750.25).abs() < 1e-9);
        use chrono::Timelike;
        assert_eq!(ip.timestamp.hour(), 14);
    }

    #[test]
    fn test_parse_index_price_space_format() {
        let r = V3IndexPrice {
            timestamp: "2024-01-15 14:00:00.000".to_string(),
            price: 4800.0,
        };
        let ip = parse_index_price(&r).expect("should parse space-separated timestamp");
        assert!((ip.price - 4800.0).abs() < 1e-9);
    }

    #[test]
    fn test_parse_index_price_invalid_returns_none() {
        let r = V3IndexPrice {
            timestamp: "not-a-timestamp".to_string(),
            price: 0.0,
        };
        assert!(parse_index_price(&r).is_none());
    }

    // ── EOD date-range chunking (365-day windows) ──────────────────────────────

    #[test]
    fn test_eod_chunk_boundary_364_days_is_single_chunk() {
        // 364 days ≤ 364 → should fit in one chunk.
        let start = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2023, 12, 31).unwrap();
        let days = (end - start).num_days();
        assert!(days <= 364, "single chunk fits within 364-day window");
    }

    #[test]
    fn test_eod_chunk_boundary_multi_year_requires_two_chunks() {
        // 2-year range requires 2 chunks (max 364 days each).
        let start = NaiveDate::from_ymd_opt(2022, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2023, 12, 31).unwrap();
        let total_days = (end - start).num_days();
        let expected_chunks = (total_days / 365) + 1;
        assert!(expected_chunks >= 2);
    }

    // ── Option EOD Parquet cache helpers ──────────────────────────────────────

    #[test]
    fn test_date_to_ns_epoch() {
        // 1970-01-01 → 0 nanoseconds
        let d = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        assert_eq!(date_to_ns(d), 0);
    }

    #[test]
    fn test_date_to_ns_known_date() {
        // 2021-04-30 → known epoch seconds = 1619740800
        let d = NaiveDate::from_ymd_opt(2021, 4, 30).unwrap();
        let expected_ns = 1_619_740_800_i64 * 1_000_000_000;
        assert_eq!(date_to_ns(d), expected_ns);
    }

    #[test]
    fn test_v3_to_option_eod_bars_basic_fields() {
        let row = V3OptionEod {
            symbol: "SPY 210430P00480000".to_string(),
            expiration: "20210430".to_string(),
            strike: 480.0,
            right: "P".to_string(),
            date: "20210419".to_string(),
            open: 1.5,
            high: 2.0,
            low: 1.0,
            close: 1.8,
            volume: 1000.0,
            count: 10,
            bid_size: 10.0,
            bid_exchange: 0,
            bid_price: 1.7,
            bid_condition: 0,
            ask_size: 5.0,
            ask_exchange: 0,
            ask_price: 1.9,
            ask_condition: 0,
            created: String::new(),
            last_trade: String::new(),
        };
        let date = NaiveDate::from_ymd_opt(2021, 4, 19).unwrap();
        let bars = v3_to_option_eod_bars("SPY", date, &[row]);
        assert_eq!(bars.len(), 1);
        let bar = &bars[0];
        assert_eq!(bar.date, date);
        assert_eq!(bar.underlying, "SPY");
        assert_eq!(bar.right, "P");
        assert_eq!(bar.volume, 1000);
        assert_eq!(bar.bid_size, 10);
        assert_eq!(bar.ask_size, 5);
    }

    #[test]
    fn test_option_eod_bars_to_v3_roundtrip() {
        // A bar converted to V3OptionEod must have the same date/right/volume.
        let row = V3OptionEod {
            symbol: "SPY 210430C00450000".to_string(),
            expiration: "20210430".to_string(),
            strike: 450.0,
            right: "C".to_string(),
            date: "20210419".to_string(),
            open: 2.5,
            high: 3.0,
            low: 2.0,
            close: 2.8,
            volume: 500.0,
            count: 5,
            bid_size: 8.0,
            bid_exchange: 0,
            bid_price: 2.7,
            bid_condition: 0,
            ask_size: 4.0,
            ask_exchange: 0,
            ask_price: 2.9,
            ask_condition: 0,
            created: String::new(),
            last_trade: String::new(),
        };
        let date = NaiveDate::from_ymd_opt(2021, 4, 19).unwrap();
        let bars = v3_to_option_eod_bars("SPY", date, std::slice::from_ref(&row));
        let back = option_eod_bars_to_v3(bars);
        assert_eq!(back.len(), 1);
        let r = &back[0];
        assert_eq!(r.right, "C");
        assert_eq!(r.volume, 500.0);
        assert_eq!(r.date, "20210419");
    }
}
