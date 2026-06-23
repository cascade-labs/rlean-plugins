use anyhow::{bail, Result};
use lean_core::{DateTime, Resolution, SecurityType, TimeSpan};
use lean_data::{TradeBar, TradeBarData};
use lean_data_providers::{DataType, HistoryRequest, IHistoryProvider};
use lean_plugin::ensure_crypto_provider;
use reqwest::blocking::Client;
use reqwest::StatusCode;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::time::Duration;
use tracing::debug;

use crate::config::{LIVE_BASE, SANDBOX_BASE};
use crate::models::{TradierQuote, TradierQuoteContainer};

pub struct TradierHistoryProvider {
    http: Client,
    base_url: String,
    access_token: String,
}

impl TradierHistoryProvider {
    pub fn new(access_token: String, use_sandbox: bool) -> Self {
        ensure_crypto_provider();

        Self {
            http: Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("failed to build reqwest blocking client"),
            base_url: if use_sandbox { SANDBOX_BASE } else { LIVE_BASE }.to_string(),
            access_token,
        }
    }

    fn get_quotes(&self, symbols: &[&str]) -> Result<Vec<TradierQuote>> {
        if symbols.is_empty() {
            return Ok(Vec::new());
        }

        let csv = symbols.join(",");
        let url = format!(
            "{}/markets/quotes?symbols={csv}&greeks=false",
            self.base_url
        );
        let resp = self
            .http
            .get(&url)
            .bearer_auth(&self.access_token)
            .header("Accept", "application/json")
            .send()?;
        check_status(resp.status())?;
        let container: TradierQuoteContainer = resp.json()?;
        Ok(normalize_quote_list(container))
    }
}

#[async_trait::async_trait]
impl IHistoryProvider for TradierHistoryProvider {
    async fn get_history(&self, request: &HistoryRequest) -> Result<Vec<TradeBar>> {
        if request.data_type != DataType::TradeBar {
            return Ok(Vec::new());
        }
        if request.symbol.security_type() != SecurityType::Equity {
            return Ok(Vec::new());
        }

        let ticker = request.symbol.value.as_str();
        let quotes = self.get_quotes(&[ticker])?;
        let Some(quote) = quotes
            .into_iter()
            .find(|quote| quote.symbol.eq_ignore_ascii_case(ticker))
        else {
            debug!("Tradier history seed: no quote returned for {ticker}");
            return Ok(Vec::new());
        };

        let Some(bar) = quote_to_trade_bar(request, quote) else {
            debug!("Tradier history seed: zero quote returned for {ticker}");
            return Ok(Vec::new());
        };
        Ok(vec![bar])
    }
}

fn check_status(status: StatusCode) -> Result<()> {
    if status == StatusCode::UNAUTHORIZED {
        bail!("Tradier: unauthorized (check access token)");
    }
    if !status.is_success() {
        bail!("Tradier HTTP error: {status}");
    }
    Ok(())
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

fn quote_to_trade_bar(request: &HistoryRequest, quote: TradierQuote) -> Option<TradeBar> {
    let price = quote_price(&quote)?;
    let open = positive_decimal(quote.open).unwrap_or(price);
    let close = positive_decimal(quote.close).unwrap_or(price);
    let high = positive_decimal(quote.high)
        .unwrap_or(price)
        .max(open)
        .max(close);
    let low = positive_decimal(quote.low)
        .unwrap_or(price)
        .min(open)
        .min(close);
    let volume = Decimal::from_i64(quote.volume.max(0)).unwrap_or(Decimal::ZERO);
    let period = request.resolution.to_time_span().unwrap_or(TimeSpan::ZERO);
    let time = bar_time(request.end, request.resolution, period);

    Some(TradeBar::new(
        request.symbol.clone(),
        time,
        period,
        TradeBarData::new(open, high, low, close, volume),
    ))
}

fn quote_price(quote: &TradierQuote) -> Option<Decimal> {
    positive_decimal(quote.last)
        .or_else(
            || match (positive_decimal(quote.bid), positive_decimal(quote.ask)) {
                (Some(bid), Some(ask)) => Some((bid + ask) / Decimal::from(2)),
                _ => None,
            },
        )
        .or_else(|| positive_decimal(quote.close))
        .or_else(|| positive_decimal(quote.open))
}

fn positive_decimal(value: f64) -> Option<Decimal> {
    if value <= 0.0 || !value.is_finite() {
        return None;
    }
    Decimal::from_f64(value)
}

fn bar_time(end: DateTime, resolution: Resolution, period: TimeSpan) -> DateTime {
    if matches!(resolution, Resolution::Tick) {
        end
    } else {
        end - period
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use lean_core::{Market, Symbol};

    fn request() -> HistoryRequest {
        HistoryRequest {
            symbol: Symbol::create_equity("JOBY", &Market::usa()),
            resolution: Resolution::Minute,
            start: DateTime::from(Utc.with_ymd_and_hms(2026, 6, 23, 15, 0, 0).unwrap()),
            end: DateTime::from(Utc.with_ymd_and_hms(2026, 6, 23, 15, 1, 0).unwrap()),
            data_type: DataType::TradeBar,
        }
    }

    #[test]
    fn quote_to_trade_bar_uses_last_price() {
        let bar = quote_to_trade_bar(
            &request(),
            TradierQuote {
                symbol: "JOBY".to_string(),
                last: 8.25,
                bid: 8.24,
                ask: 8.26,
                volume: 1234,
                open: 8.10,
                high: 8.30,
                low: 8.00,
                close: 8.20,
            },
        )
        .expect("bar");

        assert_eq!(bar.close, Decimal::from_f64(8.20).unwrap());
        assert_eq!(bar.volume, Decimal::from_i64(1234).unwrap());
        assert_eq!(bar.end_time, request().end);
    }

    #[test]
    fn quote_to_trade_bar_falls_back_to_mid() {
        let bar = quote_to_trade_bar(
            &request(),
            TradierQuote {
                symbol: "RKT".to_string(),
                last: 0.0,
                bid: 14.00,
                ask: 14.10,
                volume: 0,
                open: 0.0,
                high: 0.0,
                low: 0.0,
                close: 0.0,
            },
        )
        .expect("bar");

        assert_eq!(bar.close, Decimal::from_f64(14.05).unwrap());
        assert_eq!(bar.open, Decimal::from_f64(14.05).unwrap());
    }

    #[test]
    fn quote_to_trade_bar_rejects_zero_quote() {
        let bar = quote_to_trade_bar(
            &request(),
            TradierQuote {
                symbol: "ZERO".to_string(),
                last: 0.0,
                bid: 0.0,
                ask: 0.0,
                volume: 0,
                open: 0.0,
                high: 0.0,
                low: 0.0,
                close: 0.0,
            },
        );

        assert!(bar.is_none());
    }
}
