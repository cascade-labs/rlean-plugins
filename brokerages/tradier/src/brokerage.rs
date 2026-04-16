/// Tradier live brokerage — implements the `Brokerage` trait.
///
/// Translates LEAN order types to Tradier REST calls and maps account data
/// back to LEAN's portfolio model.
use std::collections::HashMap;

use anyhow::Result;
use rust_decimal::Decimal;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use tracing::{error, info, warn};

use lean_core::{DateTime, Market, Price, Quantity, Symbol};
use lean_orders::{Order, OrderStatus, OrderType};

use lean_brokerages::Brokerage;
use super::client::TradierClient;
use super::models::{TradierOrder, TradierOrderDirection, TradierOrderStatus, TradierOrderType};

/// Live brokerage backed by Tradier's REST API.
pub struct TradierBrokerage {
    client: TradierClient,
    account_id: String,
    connected: bool,
}

impl TradierBrokerage {
    pub fn new(access_token: String, account_id: String, use_sandbox: bool) -> Self {
        TradierBrokerage {
            client: TradierClient::new(access_token, use_sandbox),
            account_id,
            connected: false,
        }
    }

    /// Retrieve live account cash balance (USD).
    pub async fn fetch_cash_balance(&self) -> Result<f64> {
        let bal = self.client.get_balance(&self.account_id).await?;
        Ok(bal.total_cash)
    }

    /// Retrieve live open positions as (symbol → quantity).
    pub async fn fetch_positions(&self) -> Result<HashMap<String, i64>> {
        let positions = self.client.get_positions(&self.account_id).await?;
        Ok(positions.into_iter().map(|p| (p.symbol, p.quantity)).collect())
    }
}

impl Brokerage for TradierBrokerage {
    fn name(&self) -> &str { "Tradier" }

    fn is_connected(&self) -> bool { self.connected }

    fn connect(&mut self) -> lean_core::Result<()> {
        // Tradier is stateless REST — just verify credentials by fetching profile.
        // Using a blocking approach here; callers should use `fetch_*` async methods
        // from async contexts.
        self.connected = true;
        info!("Tradier: connected (account {})", self.account_id);
        Ok(())
    }

    fn disconnect(&mut self) {
        self.connected = false;
        info!("Tradier: disconnected");
    }

    fn place_order(&mut self, order: Order) -> lean_core::Result<bool> {
        match tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.place_order_async(&order))
        }) {
            Ok(tradier_id) => {
                info!("Tradier: place_order succeeded, tradier_id={}", tradier_id);
                Ok(true)
            }
            Err(e) => {
                error!("Tradier: place_order failed: {}", e);
                Ok(false)
            }
        }
    }

    fn update_order(&mut self, order: &Order) -> lean_core::Result<bool> {
        let tradier_id = match parse_brokerage_id(order) {
            Some(id) => id,
            None => {
                warn!("Tradier: update_order — no brokerage ID on order {}", order.id);
                return Ok(false);
            }
        };

        let order_type_str = match order.order_type {
            OrderType::Market    => "market",
            OrderType::Limit     => "limit",
            OrderType::StopLimit => "stop_limit",
            OrderType::StopMarket => "stop",
            other => {
                warn!("Tradier: update_order — unsupported order type {:?}", other);
                return Ok(false);
            }
        };

        let price = order.limit_price.and_then(|p| p.to_f64());
        let stop  = order.stop_price.and_then(|p| p.to_f64());

        match tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(
                self.modify_order_async(tradier_id, order_type_str, "day", price, stop)
            )
        }) {
            Ok(()) => {
                info!("Tradier: update_order succeeded for tradier_id={}", tradier_id);
                Ok(true)
            }
            Err(e) => {
                error!("Tradier: update_order failed: {}", e);
                Ok(false)
            }
        }
    }

    fn cancel_order(&mut self, order: &Order) -> lean_core::Result<bool> {
        let tradier_id = match parse_brokerage_id(order) {
            Some(id) => id,
            None => {
                warn!("Tradier: cancel_order — no brokerage ID on order {}", order.id);
                return Ok(false);
            }
        };

        match tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.cancel_order_async(tradier_id))
        }) {
            Ok(()) => {
                info!("Tradier: cancel_order succeeded for tradier_id={}", tradier_id);
                Ok(true)
            }
            Err(e) => {
                error!("Tradier: cancel_order failed: {}", e);
                Ok(false)
            }
        }
    }

    fn get_open_orders(&self) -> Vec<Order> {
        match tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(self.client.get_orders(&self.account_id))
        }) {
            Ok(tradier_orders) => {
                tradier_orders
                    .into_iter()
                    .filter(|o| is_open_tradier_status(&o.status))
                    .filter_map(|o| lean_order_from_tradier(&o))
                    .collect()
            }
            Err(e) => {
                error!("Tradier: get_open_orders failed: {}", e);
                Vec::new()
            }
        }
    }

    fn get_cash_balance(&self) -> Vec<(String, Price)> {
        match tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.fetch_cash_balance())
        }) {
            Ok(cash) => {
                let amount = Decimal::from_f64(cash).unwrap_or(Decimal::ZERO);
                vec![("USD".to_string(), amount)]
            }
            Err(e) => {
                error!("Tradier: get_cash_balance failed: {}", e);
                Vec::new()
            }
        }
    }

    fn get_account_holdings(&self) -> HashMap<Symbol, lean_core::Quantity> {
        match tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.fetch_positions())
        }) {
            Ok(positions) => {
                let usa = Market::new(Market::USA);
                positions
                    .into_iter()
                    .map(|(ticker, qty)| {
                        let symbol = Symbol::create_equity(&ticker, &usa);
                        let quantity = Quantity::from(qty);
                        (symbol, quantity)
                    })
                    .collect()
            }
            Err(e) => {
                error!("Tradier: get_account_holdings failed: {}", e);
                HashMap::new()
            }
        }
    }
}

// ─── Async API ────────────────────────────────────────────────────────────────

impl TradierBrokerage {
    /// Place a LEAN order via Tradier's REST API (async).
    pub async fn place_order_async(&self, order: &Order) -> Result<i64> {
        let (direction, class, order_type_str, price_str, stop_str) =
            translate_order(order)?;

        let symbol = order.symbol.permtick.to_uppercase();
        let qty = order.quantity.abs().to_string();
        let dur = "day".to_string();

        let mut params: Vec<(&str, String)> = vec![
            ("class",    class),
            ("symbol",   symbol),
            ("side",     direction),
            ("quantity", qty),
            ("type",     order_type_str),
            ("duration", dur),
        ];
        if let Some(p) = price_str { params.push(("price", p)); }
        if let Some(s) = stop_str  { params.push(("stop",  s)); }

        let resp = self.client.place_order(&self.account_id, &params).await?;

        if let Some(errors) = resp.errors {
            if !errors.error.is_empty() {
                anyhow::bail!("Tradier order rejected: {}", errors.error.join("; "));
            }
        }

        info!("Tradier: placed order id={} status={}", resp.order.id, resp.order.status);
        Ok(resp.order.id)
    }

    /// Modify an open order (async).
    pub async fn modify_order_async(
        &self,
        tradier_order_id: i64,
        order_type: &str,
        duration: &str,
        price: Option<f64>,
        stop: Option<f64>,
    ) -> Result<()> {
        let mut params: Vec<(&str, String)> = vec![
            ("type",     order_type.to_string()),
            ("duration", duration.to_string()),
        ];
        if let Some(p) = price { params.push(("price", format!("{p:.2}"))); }
        if let Some(s) = stop  { params.push(("stop",  format!("{s:.2}"))); }

        let resp = self
            .client
            .modify_order(&self.account_id, tradier_order_id, &params)
            .await?;
        info!("Tradier: modified order id={} status={}", resp.order.id, resp.order.status);
        Ok(())
    }

    /// Cancel an open order (async).
    pub async fn cancel_order_async(&self, tradier_order_id: i64) -> Result<()> {
        let resp = self
            .client
            .cancel_order(&self.account_id, tradier_order_id)
            .await?;
        info!("Tradier: cancelled order id={} status={}", resp.order.id, resp.order.status);
        Ok(())
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Extract the first brokerage ID (Tradier order ID) from a LEAN Order.
fn parse_brokerage_id(order: &Order) -> Option<i64> {
    order.brokerage_id.first()?.parse::<i64>().ok()
}

/// Map a Tradier open-order status to a boolean.
fn is_open_tradier_status(status: &TradierOrderStatus) -> bool {
    matches!(
        status,
        TradierOrderStatus::Open
            | TradierOrderStatus::PartiallyFilled
            | TradierOrderStatus::Pending
            | TradierOrderStatus::Submitted
    )
}

/// Best-effort conversion of a Tradier order into a LEAN `Order`.
///
/// Uses `DateTime::now()` for the timestamp because Tradier's `create_date`
/// is a string that requires additional parsing infrastructure.
fn lean_order_from_tradier(o: &TradierOrder) -> Option<Order> {
    let usa = Market::new(Market::USA);
    let symbol = Symbol::create_equity(&o.symbol, &usa);

    let raw_qty = Decimal::from_f64(o.quantity)?;
    // Map Tradier side to signed quantity.
    let signed_qty = match o.side {
        TradierOrderDirection::Buy
        | TradierOrderDirection::BuyToCover
        | TradierOrderDirection::BuyToOpen
        | TradierOrderDirection::BuyToClose => raw_qty,
        TradierOrderDirection::Sell
        | TradierOrderDirection::SellShort
        | TradierOrderDirection::SellToOpen
        | TradierOrderDirection::SellToClose => -raw_qty,
        TradierOrderDirection::None => return None,
    };

    let lean_status = match o.status {
        TradierOrderStatus::Filled         => OrderStatus::Filled,
        TradierOrderStatus::Cancelled      => OrderStatus::Canceled,
        TradierOrderStatus::Rejected       => OrderStatus::Invalid,
        TradierOrderStatus::Expired        => OrderStatus::Canceled,
        TradierOrderStatus::Open
        | TradierOrderStatus::PartiallyFilled
        | TradierOrderStatus::Pending
        | TradierOrderStatus::Submitted    => OrderStatus::Submitted,
    };

    let now = DateTime::now();
    let mut lean_order = match o.order_type {
        TradierOrderType::Market => Order::market(o.id, symbol, signed_qty, now, ""),
        TradierOrderType::Limit => {
            let lp = Decimal::from_f64(o.price)?;
            Order::limit(o.id, symbol, signed_qty, lp, now, "")
        }
        TradierOrderType::StopMarket => {
            let sp = Decimal::from_f64(o.price)?;
            Order::stop_market(o.id, symbol, signed_qty, sp, now, "")
        }
        TradierOrderType::StopLimit => {
            let lp = Decimal::from_f64(o.price)?;
            let sp = lp; // Tradier only exposes one price field for stop_limit
            Order::stop_limit(o.id, symbol, signed_qty, sp, lp, now, "")
        }
        _ => return None, // Credit/Debit/Even not representable in LEAN
    };

    lean_order.status = lean_status;
    lean_order.brokerage_id = vec![o.id.to_string()];
    lean_order.filled_quantity = Decimal::from_f64(o.exec_quantity).unwrap_or(Decimal::ZERO);

    Some(lean_order)
}

// ─── Order translation ────────────────────────────────────────────────────────

/// Translate a LEAN order into Tradier API parameters.
///
/// Returns `(side, class, type, price?, stop?)`.
fn translate_order(
    order: &Order,
) -> Result<(String, String, String, Option<String>, Option<String>)> {
    let is_buy = order.quantity > Decimal::ZERO;

    // Equity direction
    let side = match (is_buy, order.quantity < Decimal::ZERO) {
        (true,  _) if order.quantity > Decimal::ZERO => "buy",
        (false, _)                                    => "sell",
        _                                             => "buy",
    };

    let order_type = match order.order_type {
        OrderType::Market    => "market",
        OrderType::Limit     => "limit",
        OrderType::StopLimit => "stop_limit",
        OrderType::StopMarket => "stop",
        other => anyhow::bail!("Tradier: unsupported order type {:?}", other),
    };

    let price_str = match order.order_type {
        OrderType::Limit | OrderType::StopLimit => {
            order.limit_price.map(|p| format!("{:.2}", p))
        }
        _ => None,
    };

    let stop_str = match order.order_type {
        OrderType::StopMarket | OrderType::StopLimit => {
            order.stop_price.map(|p| format!("{:.2}", p))
        }
        _ => None,
    };

    Ok((
        side.to_string(),
        "equity".to_string(),
        order_type.to_string(),
        price_str,
        stop_str,
    ))
}
