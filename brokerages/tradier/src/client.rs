/// Tradier REST API client.
///
/// Handles authentication (Bearer token), rate limiting (1 req/s standard,
/// 1 req/s data, 1 req/s orders), and JSON deserialization.
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::sync::Mutex;
use tracing::debug;

use super::models::{
    TradierBalanceContainer, TradierBalanceDetails, TradierOrder, TradierOrderResponse,
    TradierOrdersContainer, TradierPosition, TradierPositionsContainer, TradierQuote,
    TradierQuoteContainer, TradierUserProfile, TradierUserProfileContainer,
};

const LIVE_BASE: &str = "https://api.tradier.com/v1";
const SANDBOX_BASE: &str = "https://sandbox.tradier.com/v1";

/// Exposed for testing only — mirrors the private constants above.
#[cfg(test)]
pub const LIVE_BASE_FOR_TEST: &str = LIVE_BASE;
#[cfg(test)]
pub const SANDBOX_BASE_FOR_TEST: &str = SANDBOX_BASE;

/// Minimal token-bucket rate limiter (one shared timestamp per category).
struct RateLimiter {
    min_interval: Duration,
    last: Mutex<Instant>,
}

impl RateLimiter {
    fn new(rps: f64) -> Self {
        RateLimiter {
            min_interval: Duration::from_secs_f64(1.0 / rps),
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

pub struct TradierClient {
    http: Client,
    base_url: String,
    access_token: String,
    /// Rate limiter for standard account / market data calls (60 req/min = 1/s).
    standard_limiter: RateLimiter,
    /// Rate limiter for order operations (60 req/min = 1/s).
    order_limiter: RateLimiter,
}

impl TradierClient {
    /// Create a live or sandbox client.
    pub fn new(access_token: String, use_sandbox: bool) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("failed to build reqwest client");

        TradierClient {
            http,
            base_url: if use_sandbox { SANDBOX_BASE } else { LIVE_BASE }.to_string(),
            access_token,
            standard_limiter: RateLimiter::new(1.0),
            order_limiter: RateLimiter::new(1.0),
        }
    }

    // ─── Account endpoints ───────────────────────────────────────────────────

    pub async fn get_user_profile(&self) -> Result<TradierUserProfile> {
        let container: TradierUserProfileContainer = self.get_standard("user/profile").await?;
        Ok(container.profile)
    }

    pub async fn get_balance(&self, account_id: &str) -> Result<TradierBalanceDetails> {
        let path = format!("accounts/{account_id}/balances");
        let container: TradierBalanceContainer = self.get_standard(&path).await?;
        Ok(container.balances)
    }

    pub async fn get_positions(&self, account_id: &str) -> Result<Vec<TradierPosition>> {
        let path = format!("accounts/{account_id}/positions");
        let container: TradierPositionsContainer = self.get_standard(&path).await?;
        Ok(normalize_position_list(container))
    }

    pub async fn get_orders(&self, account_id: &str) -> Result<Vec<TradierOrder>> {
        let path = format!("accounts/{account_id}/orders");
        let container: TradierOrdersContainer = self.get_standard(&path).await?;
        Ok(normalize_order_list(container))
    }

    // ─── Order operations ────────────────────────────────────────────────────

    pub async fn place_order(
        &self,
        account_id: &str,
        params: &[(&str, String)],
    ) -> Result<TradierOrderResponse> {
        self.order_limiter.wait().await;
        let url = format!("{}/accounts/{}/orders", self.base_url, account_id);
        // Tradier expects form-encoded POST for orders
        let body: Vec<(String, String)> = params
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();
        let resp = self
            .http
            .post(&url)
            .bearer_auth(&self.access_token)
            .form(&body)
            .send()
            .await?;
        check_status(&resp)?;
        Ok(resp.json::<TradierOrderResponse>().await?)
    }

    pub async fn modify_order(
        &self,
        account_id: &str,
        order_id: i64,
        params: &[(&str, String)],
    ) -> Result<TradierOrderResponse> {
        self.order_limiter.wait().await;
        let url = format!(
            "{}/accounts/{}/orders/{}",
            self.base_url, account_id, order_id
        );
        let body: Vec<(String, String)> = params
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();
        let resp = self
            .http
            .put(&url)
            .bearer_auth(&self.access_token)
            .form(&body)
            .send()
            .await?;
        check_status(&resp)?;
        Ok(resp.json::<TradierOrderResponse>().await?)
    }

    pub async fn cancel_order(
        &self,
        account_id: &str,
        order_id: i64,
    ) -> Result<TradierOrderResponse> {
        self.order_limiter.wait().await;
        let url = format!(
            "{}/accounts/{}/orders/{}",
            self.base_url, account_id, order_id
        );
        let resp = self
            .http
            .delete(&url)
            .bearer_auth(&self.access_token)
            .send()
            .await?;
        check_status(&resp)?;
        Ok(resp.json::<TradierOrderResponse>().await?)
    }

    // ─── Market data ─────────────────────────────────────────────────────────

    pub async fn get_quotes(&self, symbols: &[&str]) -> Result<Vec<TradierQuote>> {
        if symbols.is_empty() {
            return Ok(Vec::new());
        }
        self.standard_limiter.wait().await;
        let csv = symbols.join(",");
        let url = format!(
            "{}/markets/quotes?symbols={}&greeks=false",
            self.base_url, csv
        );
        let resp = self
            .http
            .get(&url)
            .bearer_auth(&self.access_token)
            .header("Accept", "application/json")
            .send()
            .await?;
        check_status(&resp)?;
        let container: TradierQuoteContainer = resp.json().await?;
        Ok(normalize_quote_list(container))
    }

    // ─── Internal ────────────────────────────────────────────────────────────

    async fn get_standard<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        self.standard_limiter.wait().await;
        let url = format!("{}/{}", self.base_url, path);
        debug!("Tradier GET {url}");
        let resp = self
            .http
            .get(&url)
            .bearer_auth(&self.access_token)
            .header("Accept", "application/json")
            .send()
            .await?;
        check_status(&resp)?;
        Ok(resp.json::<T>().await?)
    }
}

fn check_status(resp: &reqwest::Response) -> Result<()> {
    let status = resp.status();
    if status == 401 {
        bail!("Tradier: unauthorized (check access token)");
    }
    if status == 429 {
        bail!("Tradier: rate limited (429)");
    }
    if !status.is_success() {
        bail!("Tradier API error: HTTP {}", status);
    }
    Ok(())
}

/// Tradier returns a single object when there is one position, and an array
/// when there are multiple.  Normalize to Vec.
fn normalize_position_list(container: TradierPositionsContainer) -> Vec<TradierPosition> {
    let wrapper = match container.positions {
        None => return Vec::new(),
        Some(w) => w,
    };
    parse_single_or_array(wrapper.position).unwrap_or_default()
}

fn normalize_order_list(container: TradierOrdersContainer) -> Vec<TradierOrder> {
    let wrapper = match container.orders {
        None => return Vec::new(),
        Some(w) => w,
    };
    parse_single_or_array(wrapper.order).unwrap_or_default()
}

fn normalize_quote_list(container: TradierQuoteContainer) -> Vec<TradierQuote> {
    let wrapper = match container.quotes {
        None => return Vec::new(),
        Some(w) => w,
    };
    parse_single_or_array(wrapper.quote).unwrap_or_default()
}

fn parse_single_or_array<T: DeserializeOwned>(v: Value) -> Option<Vec<T>> {
    match &v {
        Value::Array(_) => serde_json::from_value(v).ok(),
        Value::Object(_) => serde_json::from_value::<T>(v).ok().map(|x| vec![x]),
        _ => None,
    }
}
