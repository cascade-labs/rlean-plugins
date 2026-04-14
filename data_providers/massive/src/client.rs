use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use reqwest::Client;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use tokio::sync::Mutex;
use tracing::{info, warn};

use chrono::{NaiveDate, TimeZone, Utc};
use chrono_tz::America::New_York;
use lean_core::{DateTime, NanosecondTimestamp, Resolution, Symbol, TimeSpan};
use lean_data::TradeBar;
use serde::de::DeserializeOwned;

use crate::models::{AggregatesResponse, MassiveDividendItem, MassiveSplitItem, PaginatedResponse};

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

    async fn wait(&self) {
        let mut last = self.last.lock().await;
        let elapsed = last.elapsed();
        if elapsed < self.min_interval {
            tokio::time::sleep(self.min_interval - elapsed).await;
        }
        *last = Instant::now();
    }
}

/// Async Massive REST API client.
///
/// Handles pagination, rate limiting, and retry on 429.
pub struct MassiveRestClient {
    api_key: String,
    http: Client,
    limiter: RateLimiter,
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
            api_key,
            http,
            limiter: RateLimiter::new(requests_per_second),
        }
    }

    pub fn api_key(&self) -> &str { &self.api_key }

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
        let ticker  = symbol.permtick.to_uppercase();
        let timespan = resolution_to_timespan(resolution)?;
        let start_ms = start.0 / 1_000_000;
        let end_ms   = end.0   / 1_000_000;
        let adj_str  = if adjusted { "true" } else { "false" };

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

        let mut all_bars: Vec<TradeBar> = Vec::new();
        let mut url = initial_url;

        loop {
            self.limiter.wait().await;
            let resp = self.fetch_aggs_with_retry(&url).await?;

            if let Some(results) = resp.results {
                for bar in results {
                    let corrected_ms = correct_polygon_dst(bar.timestamp_ms);
                    let time = NanosecondTimestamp::from_millis(corrected_ms);
                    let dec = |f: f64| Decimal::from_f64(f).unwrap_or_default();
                    all_bars.push(TradeBar {
                        symbol:   symbol.clone(),
                        time,
                        end_time: NanosecondTimestamp(time.0 + period.nanos),
                        open:     dec(bar.open),
                        high:     dec(bar.high),
                        low:      dec(bar.low),
                        close:    dec(bar.close),
                        volume:   dec(bar.volume),
                        period,
                    });
                }
            }

            match resp.next_url.filter(|s| !s.is_empty()) {
                Some(next) => {
                    url = format!("{next}&apiKey={}", self.api_key);
                }
                None => break,
            }
        }

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
        let mut all: Vec<MassiveSplitItem> = Vec::new();
        self.fetch_paged(url, &mut all).await?;
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
        let mut all: Vec<MassiveDividendItem> = Vec::new();
        self.fetch_paged(url, &mut all).await?;
        // Keep only cash / special-cash dividends
        all.retain(|d| d.dividend_type == "CD" || d.dividend_type == "SC");
        Ok(all)
    }

    /// Generic paginated fetch for Massive's v3 reference endpoints.
    async fn fetch_paged<T: DeserializeOwned>(&self, initial_url: String, out: &mut Vec<T>) -> Result<()> {
        let mut url = initial_url;
        loop {
            self.limiter.wait().await;
            let resp = self.fetch_paged_with_retry::<T>(&url).await?;
            if let Some(results) = resp.results {
                out.extend(results);
            }
            match resp.next_url.filter(|s| !s.is_empty()) {
                Some(next) => url = format!("{next}&apiKey={}", self.api_key),
                None => break,
            }
        }
        Ok(())
    }

    async fn fetch_aggs_with_retry(&self, url: &str) -> Result<AggregatesResponse> {
        for attempt in 0..MAX_RETRIES {
            match self.http.get(url).send().await {
                Ok(r) if r.status() == 429 => {
                    let wait = Duration::from_secs(10 * (attempt as u64 + 1));
                    warn!("Massive: rate limited (429), waiting {:.0}s", wait.as_secs_f64());
                    tokio::time::sleep(wait).await;
                    continue;
                }
                Ok(r) if r.status().is_success() => {
                    return Ok(r.json::<AggregatesResponse>().await?);
                }
                Ok(r) => {
                    bail!("Massive API error: HTTP {}", r.status());
                }
                Err(e) if attempt + 1 < MAX_RETRIES => {
                    warn!("Massive: request error (attempt {}): {}", attempt + 1, e);
                    tokio::time::sleep(Duration::from_secs(2u64.pow(attempt))).await;
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }
        bail!("Massive: max retries ({MAX_RETRIES}) exceeded");
    }

    async fn fetch_paged_with_retry<T: DeserializeOwned>(&self, url: &str) -> Result<PaginatedResponse<T>> {
        for attempt in 0..MAX_RETRIES {
            match self.http.get(url).send().await {
                Ok(r) if r.status() == 429 => {
                    let wait = Duration::from_secs(10 * (attempt as u64 + 1));
                    warn!("Massive: rate limited (429), waiting {:.0}s", wait.as_secs_f64());
                    tokio::time::sleep(wait).await;
                    continue;
                }
                Ok(r) if r.status().is_success() => {
                    return Ok(r.json::<PaginatedResponse<T>>().await?);
                }
                Ok(r) => {
                    bail!("Massive API error: HTTP {}", r.status());
                }
                Err(e) if attempt + 1 < MAX_RETRIES => {
                    warn!("Massive: request error (attempt {}): {}", attempt + 1, e);
                    tokio::time::sleep(Duration::from_secs(2u64.pow(attempt))).await;
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }
        bail!("Massive: max retries ({MAX_RETRIES}) exceeded");
    }
}

fn resolution_to_timespan(res: Resolution) -> Result<&'static str> {
    match res {
        Resolution::Tick   => bail!("Tick resolution not supported via the aggregates API"),
        Resolution::Second => Ok("second"),
        Resolution::Minute => Ok("minute"),
        Resolution::Hour   => Ok("hour"),
        Resolution::Daily  => Ok("day"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolution_second_maps_to_second() {
        assert_eq!(resolution_to_timespan(Resolution::Second).unwrap(), "second");
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
