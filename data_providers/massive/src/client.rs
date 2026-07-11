use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use reqwest::blocking::Client;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use tracing::{info, warn};

use chrono::{NaiveDate, TimeZone, Utc};
use chrono_tz::America::New_York;
use lean_core::{DateTime, NanosecondTimestamp, Resolution, Symbol, TimeSpan};
use lean_data::TradeBar;
use serde::de::DeserializeOwned;

use crate::models::{
    AggregatesResponse, MassiveDividendItem, MassiveSplitItem, PaginatedResponse, TickerDetails,
    TickerDetailsResponse, TickerEvent, TickerEventsResponse,
};

const BASE_URL: &str = "https://api.massive.com";
const MAX_RETRIES: u32 = 5;

/// Polygon (Massive) stores minute-bar timestamps using Eastern Standard Time
/// (UTC-5) as a fixed offset year-round, ignoring Daylight Saving Time.
/// During EDT (UTC-4) months, this makes every bar appear 1 hour late.
///
/// This function corrects the millisecond timestamp:
/// - Interpret Polygon's UTC value as if it were computed with EST (UTC-5)
///   → recover the "naive Eastern time" the bar belongs to
/// - Re-convert that naive Eastern time to real UTC using DST-aware NY timezone
///   → subtract 1 hour during EDT, 0 during EST
fn correct_polygon_dst(polygon_ms: i64) -> i64 {
    // Strip Polygon's assumed 5-hour UTC offset to get "naive ET" datetime.
    let utc_dt = Utc.timestamp_millis_opt(polygon_ms).unwrap();
    let naive_et = utc_dt.naive_utc() - chrono::Duration::hours(5);
    // Interpret that naive ET time as actual New York time (DST-aware).
    match New_York.from_local_datetime(&naive_et) {
        chrono::LocalResult::Single(dt) => dt.timestamp_millis(),
        chrono::LocalResult::Ambiguous(dt, _) => dt.timestamp_millis(),
        chrono::LocalResult::None => polygon_ms, // gap (spring-forward); keep original
    }
}

/// Minimal token-bucket rate limiter using a single timestamp.
struct RateLimiter {
    min_interval: Duration,
    last: Mutex<Instant>,
}

impl RateLimiter {
    fn new(requests_per_second: f64) -> Self {
        RateLimiter {
            min_interval: Duration::from_secs_f64(1.0 / requests_per_second),
            // Treat last request as long ago so the first call goes immediately.
            last: Mutex::new(Instant::now() - Duration::from_secs(60)),
        }
    }

    /// Blocks the *calling* thread (never `.await`ed) — always call this from
    /// a dedicated worker thread spawned via [`run_blocking`], never directly
    /// from an async fn body.
    ///
    /// Reserves the next request slot while holding the lock, then releases the
    /// lock *before* sleeping so the mutex is never held across `thread::sleep`.
    /// Concurrent callers each reserve a distinct slot, so requests are still
    /// spaced at `requests_per_second`.
    fn wait(&self) {
        let target = {
            let mut last = self.last.lock().expect("rate limiter mutex poisoned");
            let now = Instant::now();
            // Reserve the next slot: at least `min_interval` after the previous
            // reservation, but never in the past.
            let slot = (*last + self.min_interval).max(now);
            *last = slot;
            slot
        };
        // Lock released; now sleep until the reserved slot if it's in the future.
        let now = Instant::now();
        if target > now {
            std::thread::sleep(target - now);
        }
    }
}

/// Runs `f` on a dedicated OS thread and awaits its result.
///
/// This plugin is loaded as a separate `cdylib` with its own statically
/// linked copy of `reqwest`/`tokio` (potentially even a different `tokio`
/// version than the host `rlean` binary — see the massive/thetadata client
/// history for why this matters). Calling `tokio::time::sleep`,
/// `Handle::current()`, or async `reqwest` from a thread that only ever
/// entered the *host's* runtime panics with "there is no reactor running".
///
/// `std::thread` + `futures::channel::oneshot` sidesteps this entirely: the
/// worker thread does purely synchronous I/O (blocking `reqwest` + sleeps),
/// and the receiving `Future` only relies on `std::task::Waker`, which is
/// runtime-agnostic and safe to poll from any executor, including the host's.
async fn run_blocking<T, F>(f: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let (tx, rx) = futures::channel::oneshot::channel();
    std::thread::Builder::new()
        .name("massive-request".to_string())
        .spawn(move || {
            let _ = tx.send(f());
        })?;
    rx.await
        .map_err(|_| anyhow::anyhow!("Massive request worker thread dropped response"))
}

/// Massive REST API client.
///
/// Handles pagination, rate limiting, and retry on 429. All actual HTTP I/O
/// happens synchronously on a dedicated thread via [`run_blocking`]; the
/// public methods are `async fn` only so callers can `.await` them without
/// stalling the runtime that's driving them.
pub struct MassiveRestClient {
    api_key: Arc<str>,
    http: Client,
    limiter: Arc<RateLimiter>,
}

impl MassiveRestClient {
    /// Create a new client.
    ///
    /// `requests_per_second` — e.g. `5.0` for the free tier,
    /// `300.0` for a paid plan.
    pub fn new(api_key: String, requests_per_second: f64) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(4)
            .build()
            .expect("failed to build reqwest client");

        MassiveRestClient {
            api_key: api_key.into(),
            http,
            limiter: Arc::new(RateLimiter::new(requests_per_second)),
        }
    }

    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    /// Fetch aggregate (OHLCV) bars for a symbol over the given UTC range.
    ///
    /// `adjusted` — pass `false` to get raw (unadjusted) prices; `true` for
    /// Massive's pre-adjusted prices.  Follows pagination automatically.
    pub async fn get_aggregates(
        &self,
        symbol: &Symbol,
        resolution: Resolution,
        start: DateTime,
        end: DateTime,
        adjusted: bool,
    ) -> Result<Vec<TradeBar>> {
        let ticker = symbol.permtick.to_uppercase();
        let timespan = resolution_to_timespan(resolution)?;
        let start_ms = start.0 / 1_000_000;
        let end_ms = end.0 / 1_000_000;
        let adj_str = if adjusted { "true" } else { "false" };

        info!(
            "Massive: downloading {} {} bars for {} ({} → {}, adjusted={})",
            timespan, ticker, symbol.value, start_ms, end_ms, adjusted
        );

        let period = resolution
            .to_duration()
            .map(|d| TimeSpan::from_nanos(d.as_nanos() as i64))
            .unwrap_or(TimeSpan::ONE_DAY);

        let initial_url = format!(
            "{BASE_URL}/v2/aggs/ticker/{ticker}/range/1/{timespan}/{start_ms}/{end_ms}\
             ?adjusted={adj_str}&limit=50000&apiKey={}",
            self.api_key
        );

        let http = self.http.clone();
        let limiter = Arc::clone(&self.limiter);
        let api_key = self.api_key.clone();
        let symbol = symbol.clone();
        let all_bars = run_blocking(move || -> Result<Vec<TradeBar>> {
            let mut all_bars: Vec<TradeBar> = Vec::new();
            let mut url = initial_url;

            loop {
                limiter.wait();
                let resp = fetch_aggs_with_retry(&http, &url)?;

                if let Some(results) = resp.results {
                    for bar in results {
                        let corrected_ms = correct_polygon_dst(bar.timestamp_ms);
                        let time = NanosecondTimestamp::from_millis(corrected_ms);
                        let dec = |f: f64| Decimal::from_f64(f).unwrap_or_default();
                        all_bars.push(TradeBar {
                            symbol: symbol.clone(),
                            time,
                            end_time: NanosecondTimestamp(time.0 + period.nanos),
                            open: dec(bar.open),
                            high: dec(bar.high),
                            low: dec(bar.low),
                            close: dec(bar.close),
                            volume: dec(bar.volume),
                            period,
                        });
                    }
                }

                match resp.next_url.filter(|s| !s.is_empty()) {
                    Some(next) => {
                        url = format!("{next}&apiKey={api_key}");
                    }
                    None => break,
                }
            }
            Ok(all_bars)
        })
        .await??;

        info!("Massive: received {} bars for {}", all_bars.len(), ticker);
        Ok(all_bars)
    }

    /// Fetch all splits for a ticker over a date range (sorted ascending by execution_date).
    pub async fn get_splits(
        &self,
        ticker: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<MassiveSplitItem>> {
        let url = format!(
            "{BASE_URL}/v3/reference/splits\
             ?ticker={ticker}&execution_date.gte={start}&execution_date.lte={end}\
             &order=asc&limit=1000&apiKey={}",
            self.api_key
        );
        let all: Vec<MassiveSplitItem> = self.fetch_paged(url).await?;
        Ok(all)
    }

    /// Fetch all cash dividends (type "CD" or "SC") for a ticker over a date range.
    pub async fn get_dividends(
        &self,
        ticker: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<MassiveDividendItem>> {
        let url = format!(
            "{BASE_URL}/v3/reference/dividends\
             ?ticker={ticker}&ex_dividend_date.gte={start}&ex_dividend_date.lte={end}\
             &order=asc&limit=1000&apiKey={}",
            self.api_key
        );
        let mut all: Vec<MassiveDividendItem> = self.fetch_paged(url).await?;
        // Keep only cash / special-cash dividends
        all.retain(|d| d.dividend_type == "CD" || d.dividend_type == "SC");
        Ok(all)
    }

    /// Generic paginated fetch for Massive's v3 reference endpoints.
    async fn fetch_paged<T>(&self, initial_url: String) -> Result<Vec<T>>
    where
        T: DeserializeOwned + Send + 'static,
    {
        let http = self.http.clone();
        let limiter = Arc::clone(&self.limiter);
        let api_key = self.api_key.clone();
        run_blocking(move || -> Result<Vec<T>> {
            let mut out = Vec::new();
            let mut url = initial_url;
            loop {
                limiter.wait();
                let resp = fetch_paged_with_retry::<T>(&http, &url)?;
                if let Some(results) = resp.results {
                    out.extend(results);
                }
                match resp.next_url.filter(|s| !s.is_empty()) {
                    Some(next) => url = format!("{next}&apiKey={api_key}"),
                    None => break,
                }
            }
            Ok(out)
        })
        .await?
    }

    /// Fetch ticker details (listing date, delisting date, active status).
    pub async fn get_ticker_details(&self, ticker: &str) -> Result<Option<TickerDetails>> {
        let url = format!(
            "{BASE_URL}/v3/reference/tickers/{ticker}?apiKey={}",
            self.api_key
        );
        let http = self.http.clone();
        let limiter = Arc::clone(&self.limiter);
        run_blocking(move || -> Result<Option<TickerDetails>> {
            limiter.wait();
            for attempt in 0..MAX_RETRIES {
                match http.get(&url).send() {
                    Ok(r) if r.status() == 404 => return Ok(None),
                    Ok(r) if r.status() == 429 => {
                        let wait = Duration::from_secs(10 * (attempt as u64 + 1));
                        warn!(
                            "Massive: rate limited (429), waiting {:.0}s",
                            wait.as_secs_f64()
                        );
                        std::thread::sleep(wait);
                        continue;
                    }
                    Ok(r) if r.status().is_success() => {
                        let resp = r.json::<TickerDetailsResponse>()?;
                        return Ok(resp.results);
                    }
                    Ok(r) => bail!("Massive API error: HTTP {}", r.status()),
                    Err(e) if attempt + 1 < MAX_RETRIES => {
                        warn!("Massive: request error (attempt {}): {}", attempt + 1, e);
                        std::thread::sleep(Duration::from_secs(2u64.pow(attempt)));
                        continue;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            bail!("Massive: max retries ({MAX_RETRIES}) exceeded");
        })
        .await?
    }

    /// Fetch ticker-change events for a ticker, CUSIP, or Composite FIGI.
    pub async fn get_ticker_events(&self, id: &str) -> Result<Vec<TickerEvent>> {
        let url = format!(
            "{BASE_URL}/vX/reference/tickers/{id}/events?types=ticker_change&apiKey={}",
            self.api_key
        );
        let http = self.http.clone();
        let limiter = Arc::clone(&self.limiter);
        run_blocking(move || -> Result<Vec<TickerEvent>> {
            limiter.wait();
            for attempt in 0..MAX_RETRIES {
                match http.get(&url).send() {
                    Ok(r) if r.status() == 404 => return Ok(Vec::new()),
                    Ok(r) if r.status() == 429 => {
                        let wait = Duration::from_secs(10 * (attempt as u64 + 1));
                        warn!(
                            "Massive: rate limited (429), waiting {:.0}s",
                            wait.as_secs_f64()
                        );
                        std::thread::sleep(wait);
                        continue;
                    }
                    Ok(r) if r.status().is_success() => {
                        let resp = r.json::<TickerEventsResponse>()?;
                        return Ok(resp.results.map(|r| r.events).unwrap_or_default());
                    }
                    Ok(r) => bail!("Massive API error: HTTP {}", r.status()),
                    Err(e) if attempt + 1 < MAX_RETRIES => {
                        warn!("Massive: request error (attempt {}): {}", attempt + 1, e);
                        std::thread::sleep(Duration::from_secs(2u64.pow(attempt)));
                        continue;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            bail!("Massive: max retries ({MAX_RETRIES}) exceeded");
        })
        .await?
    }
}

/// Blocking — only ever call from a thread spawned via [`run_blocking`].
fn fetch_aggs_with_retry(http: &Client, url: &str) -> Result<AggregatesResponse> {
    for attempt in 0..MAX_RETRIES {
        match http.get(url).send() {
            Ok(r) if r.status() == 429 => {
                let wait = Duration::from_secs(10 * (attempt as u64 + 1));
                warn!(
                    "Massive: rate limited (429), waiting {:.0}s",
                    wait.as_secs_f64()
                );
                std::thread::sleep(wait);
                continue;
            }
            Ok(r) if r.status().is_success() => {
                let resp = r.json::<AggregatesResponse>()?;
                // Massive returns HTTP 200 with `status:"ERROR"` for bad
                // requests (e.g. an out-of-range `from` timestamp). Treating
                // that as an empty result silently zeroed dividend factors,
                // so surface it as a hard error instead.
                if resp.status.eq_ignore_ascii_case("ERROR") {
                    bail!("Massive aggregates error status for {}", url);
                }
                return Ok(resp);
            }
            Ok(r) => {
                bail!("Massive API error: HTTP {}", r.status());
            }
            Err(e) if attempt + 1 < MAX_RETRIES => {
                warn!("Massive: request error (attempt {}): {}", attempt + 1, e);
                std::thread::sleep(Duration::from_secs(2u64.pow(attempt)));
                continue;
            }
            Err(e) => return Err(e.into()),
        }
    }
    bail!("Massive: max retries ({MAX_RETRIES}) exceeded");
}

/// Blocking — only ever call from a thread spawned via [`run_blocking`].
fn fetch_paged_with_retry<T: DeserializeOwned>(
    http: &Client,
    url: &str,
) -> Result<PaginatedResponse<T>> {
    for attempt in 0..MAX_RETRIES {
        match http.get(url).send() {
            Ok(r) if r.status() == 429 => {
                let wait = Duration::from_secs(10 * (attempt as u64 + 1));
                warn!(
                    "Massive: rate limited (429), waiting {:.0}s",
                    wait.as_secs_f64()
                );
                std::thread::sleep(wait);
                continue;
            }
            Ok(r) if r.status().is_success() => {
                let resp = r.json::<PaginatedResponse<T>>()?;
                // Massive returns HTTP 200 with `status:"ERROR"` for rejected
                // requests on the reference (splits/dividends) endpoints too.
                // Deserializing yields `results: None`, which is
                // indistinguishable from "no corporate actions" and would
                // silently drop split/dividend factors (issue #27). Surface it
                // as a hard error so the framework can retry rather than caching
                // a bogus empty factor file.
                if resp.status.eq_ignore_ascii_case("ERROR") {
                    bail!("Massive reference error status for {}", url);
                }
                return Ok(resp);
            }
            Ok(r) => {
                bail!("Massive API error: HTTP {}", r.status());
            }
            Err(e) if attempt + 1 < MAX_RETRIES => {
                warn!("Massive: request error (attempt {}): {}", attempt + 1, e);
                std::thread::sleep(Duration::from_secs(2u64.pow(attempt)));
                continue;
            }
            Err(e) => return Err(e.into()),
        }
    }
    bail!("Massive: max retries ({MAX_RETRIES}) exceeded");
}

fn resolution_to_timespan(res: Resolution) -> Result<&'static str> {
    match res {
        Resolution::Tick => bail!("Tick resolution not supported via the aggregates API"),
        Resolution::Second => Ok("second"),
        Resolution::Minute => Ok("minute"),
        Resolution::Hour => Ok("hour"),
        Resolution::Daily => Ok("day"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolution_second_maps_to_second() {
        assert_eq!(
            resolution_to_timespan(Resolution::Second).unwrap(),
            "second"
        );
    }

    #[test]
    fn resolution_tick_returns_error() {
        assert!(resolution_to_timespan(Resolution::Tick).is_err());
    }

    #[test]
    fn client_stores_api_key() {
        let client = MassiveRestClient::new("test_api_key_123".into(), 5.0);
        assert_eq!(client.api_key(), "test_api_key_123");
    }

    #[test]
    fn aggregates_url_uses_massive_base() {
        let url = format!(
            "{BASE_URL}/v2/aggs/ticker/AAPL/range/1/minute/0/1000\
             ?adjusted=false&limit=50000&apiKey=test_key"
        );
        assert!(url.contains("api.massive.com"));
    }
}
