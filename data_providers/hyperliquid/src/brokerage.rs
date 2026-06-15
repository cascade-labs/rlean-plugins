use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;

use lean_brokerages::Brokerage;
use lean_core::{LeanError, Market, Price, Result as LeanResult, SecurityType, Symbol};
use lean_orders::{Order, OrderStatus};
use reqwest::blocking::Client;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_json::{json, Value};
use tracing::{debug, info};

const DEFAULT_INFO_URL: &str = "https://api.hyperliquid.xyz/info";

#[derive(Debug, Clone)]
pub struct HyperliquidBrokerageConfig {
    pub info_url: String,
    pub wallet_address: Option<String>,
    pub paper_fill_mode: bool,
}

impl HyperliquidBrokerageConfig {
    pub fn from_json(config: &Value) -> Self {
        let info_url = config_string(config, "info_url")
            .or_else(|| std::env::var("HYPERLIQUID_INFO_URL").ok())
            .unwrap_or_else(|| DEFAULT_INFO_URL.to_string());
        let wallet_address = config_string(config, "wallet_address")
            .or_else(|| config_string(config, "address"))
            .or_else(|| std::env::var("HYPERLIQUID_WALLET_ADDRESS").ok())
            .or_else(|| std::env::var("HYPERLIQUID_ADDRESS").ok());
        let paper_fill_mode = config["paper_fill_mode"]
            .as_bool()
            .or_else(|| {
                std::env::var("HYPERLIQUID_PAPER_FILL_MODE")
                    .ok()
                    .and_then(|raw| parse_bool(&raw))
            })
            .unwrap_or(true);
        Self {
            info_url,
            wallet_address,
            paper_fill_mode,
        }
    }
}

pub struct HyperliquidBrokerage {
    config: HyperliquidBrokerageConfig,
    client: Client,
    connected: bool,
    open_orders: HashMap<i64, Order>,
    cash_balances: Vec<(String, Price)>,
    holdings: HashMap<Symbol, Decimal>,
}

impl HyperliquidBrokerage {
    pub fn new(config: HyperliquidBrokerageConfig) -> LeanResult<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .map_err(|error| LeanError::BrokerageError(error.to_string()))?;
        Ok(Self {
            config,
            client,
            connected: false,
            open_orders: HashMap::new(),
            cash_balances: Vec::new(),
            holdings: HashMap::new(),
        })
    }

    fn post_info(&self, payload: Value) -> LeanResult<Value> {
        let response = self
            .client
            .post(&self.config.info_url)
            .json(&payload)
            .send()
            .map_err(|error| {
                LeanError::BrokerageError(format!(
                    "failed to call Hyperliquid Info API {}: {error}",
                    self.config.info_url
                ))
            })?;
        let response = response.error_for_status().map_err(|error| {
            LeanError::BrokerageError(format!("Hyperliquid Info API returned error: {error}"))
        })?;
        response.json::<Value>().map_err(|error| {
            LeanError::BrokerageError(format!(
                "failed to parse Hyperliquid Info response: {error}"
            ))
        })
    }

    fn refresh_account_state(&mut self) -> LeanResult<()> {
        let Some(wallet_address) = self.config.wallet_address.as_deref() else {
            self.post_info(json!({ "type": "metaAndAssetCtxs" }))?;
            self.cash_balances.clear();
            self.holdings.clear();
            debug!("Connected Hyperliquid brokerage without wallet address");
            return Ok(());
        };

        let state = self.post_info(json!({
            "type": "clearinghouseState",
            "user": wallet_address,
        }))?;
        self.cash_balances = parse_cash_balances(&state);
        self.holdings = parse_holdings(&state);
        debug!(
            "Refreshed Hyperliquid brokerage state: cash_balances={} holdings={}",
            self.cash_balances.len(),
            self.holdings.len()
        );
        Ok(())
    }

    fn ensure_connected(&self) -> LeanResult<()> {
        if self.connected {
            Ok(())
        } else {
            Err(LeanError::BrokerageError(
                "Hyperliquid brokerage is not connected".to_string(),
            ))
        }
    }

    fn ensure_supported_order(&self, order: &Order) -> LeanResult<()> {
        if order.symbol.market().as_str() != Market::HYPERLIQUID {
            return Err(LeanError::Unsupported(format!(
                "Hyperliquid brokerage does not support market {} for {}",
                order.symbol.market(),
                order.symbol
            )));
        }
        if !matches!(
            order.symbol.security_type(),
            SecurityType::Crypto | SecurityType::CryptoFuture
        ) {
            return Err(LeanError::Unsupported(format!(
                "Hyperliquid brokerage only supports crypto symbols, got {:?}",
                order.symbol.security_type()
            )));
        }
        Ok(())
    }
}

impl Brokerage for HyperliquidBrokerage {
    fn name(&self) -> &str {
        "Hyperliquid"
    }

    fn is_connected(&self) -> bool {
        self.connected
    }

    fn connect(&mut self) -> LeanResult<()> {
        if !self.config.paper_fill_mode {
            return Err(LeanError::Unsupported(
                "Hyperliquid live exchange order routing is not enabled; use paper_fill_mode"
                    .to_string(),
            ));
        }
        self.refresh_account_state()?;
        self.connected = true;
        info!("Connected Hyperliquid brokerage in paper fill mode");
        Ok(())
    }

    fn disconnect(&mut self) {
        self.connected = false;
        self.open_orders.clear();
    }

    fn place_order(&mut self, order: Order) -> LeanResult<bool> {
        self.ensure_connected()?;
        self.ensure_supported_order(&order)?;
        let mut submitted = order;
        submitted.status = OrderStatus::Submitted;
        info!(
            "Hyperliquid paper-fill brokerage accepted order_id={} symbol={} quantity={} type={:?}",
            submitted.id, submitted.symbol.value, submitted.quantity, submitted.order_type
        );
        self.open_orders.insert(submitted.id, submitted);
        Ok(true)
    }

    fn update_order(&mut self, order: &Order) -> LeanResult<bool> {
        self.ensure_connected()?;
        self.ensure_supported_order(order)?;
        if let Some(existing) = self.open_orders.get_mut(&order.id) {
            let mut updated = order.clone();
            updated.status = OrderStatus::UpdateSubmitted;
            *existing = updated;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn cancel_order(&mut self, order: &Order) -> LeanResult<bool> {
        self.ensure_connected()?;
        self.ensure_supported_order(order)?;
        Ok(self.open_orders.remove(&order.id).is_some())
    }

    fn get_open_orders(&self) -> Vec<Order> {
        self.open_orders
            .values()
            .filter(|order| order.is_open())
            .cloned()
            .collect()
    }

    fn get_cash_balance(&self) -> Vec<(String, Price)> {
        self.cash_balances.clone()
    }

    fn get_account_holdings(&self) -> HashMap<Symbol, Decimal> {
        self.holdings.clone()
    }
}

fn config_string(config: &Value, key: &str) -> Option<String> {
    config[key]
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn parse_bool(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "y" => Some(true),
        "0" | "false" | "no" | "n" => Some(false),
        _ => None,
    }
}

fn json_decimal(value: &Value) -> Option<Decimal> {
    match value {
        Value::String(raw) => Decimal::from_str(raw).ok(),
        Value::Number(number) => Decimal::from_str(&number.to_string()).ok(),
        _ => None,
    }
}

fn parse_cash_balances(state: &Value) -> Vec<(String, Price)> {
    let account_value = state
        .pointer("/marginSummary/accountValue")
        .and_then(json_decimal)
        .or_else(|| {
            state
                .pointer("/crossMarginSummary/accountValue")
                .and_then(json_decimal)
        })
        .unwrap_or(dec!(0));
    vec![("USDC".to_string(), account_value)]
}

fn parse_holdings(state: &Value) -> HashMap<Symbol, Decimal> {
    let mut holdings = HashMap::new();
    let Some(positions) = state["assetPositions"].as_array() else {
        return holdings;
    };

    for entry in positions {
        let Some(position) = entry.get("position") else {
            continue;
        };
        let Some(coin) = position["coin"].as_str() else {
            continue;
        };
        let Some(quantity) = position.get("szi").and_then(json_decimal) else {
            continue;
        };
        if quantity.is_zero() {
            continue;
        }
        let symbol = Symbol::create_crypto_future(coin, &Market::hyperliquid());
        holdings.insert(symbol, quantity);
    }
    holdings
}

#[cfg(test)]
mod tests {
    use super::*;
    use lean_core::DateTime;
    use rust_decimal_macros::dec;

    #[test]
    fn parses_account_state() {
        let state = json!({
            "marginSummary": { "accountValue": "1234.56" },
            "assetPositions": [
                { "position": { "coin": "BTC", "szi": "0.25" } },
                { "position": { "coin": "ETH", "szi": "0" } }
            ]
        });

        let cash = parse_cash_balances(&state);
        assert_eq!(cash, vec![("USDC".to_string(), dec!(1234.56))]);
        let holdings = parse_holdings(&state);
        assert_eq!(holdings.len(), 1);
        assert!(
            holdings
                .keys()
                .any(|symbol| symbol.value == "BTC"
                    && symbol.market().as_str() == Market::HYPERLIQUID)
        );
    }

    #[test]
    fn paper_place_order_records_open_order() {
        let config = HyperliquidBrokerageConfig {
            info_url: DEFAULT_INFO_URL.to_string(),
            wallet_address: None,
            paper_fill_mode: true,
        };
        let mut brokerage = HyperliquidBrokerage::new(config).unwrap();
        brokerage.connected = true;

        let symbol = Symbol::create_crypto_future("XYZ:SP500", &Market::hyperliquid());
        let order = Order::market(1, symbol, dec!(2), DateTime::EPOCH, "test");

        assert!(brokerage.place_order(order).unwrap());
        let open = brokerage.get_open_orders();
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].status, OrderStatus::Submitted);
    }
}
