/// Tradier live brokerage — implements the `Brokerage` trait.
///
/// Translates LEAN order types to Tradier REST calls and maps account data
/// back to LEAN's portfolio model.
use std::collections::HashMap;
use std::future::Future;

use anyhow::Result;
use chrono::Timelike;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use tracing::{error, info, warn};

use lean_core::{
    DateTime, Market, OptionRight, OptionStyle, Price, Quantity, SecurityType, Symbol,
    SymbolOptionsExt,
};
use lean_orders::{Order, OrderStatus, OrderType, TimeInForce, UpdateOrderRequest};

use super::client::TradierClient;
use super::config::TradierEnvironment;
use super::models::{TradierOrder, TradierOrderDirection, TradierOrderStatus, TradierOrderType};
use lean_brokerages::{Brokerage, BrokerageHolding};

/// Live brokerage backed by Tradier's REST API.
pub struct TradierBrokerage {
    client: TradierClient,
    account_id: String,
    environment: TradierEnvironment,
    connected: bool,
}

impl TradierBrokerage {
    pub fn new(access_token: String, account_id: String, environment: TradierEnvironment) -> Self {
        TradierBrokerage {
            client: TradierClient::new(access_token, environment),
            account_id,
            environment,
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
        Ok(positions
            .into_iter()
            .map(|p| (p.symbol, p.quantity))
            .collect())
    }
}

impl Brokerage for TradierBrokerage {
    fn name(&self) -> &str {
        match self.environment {
            TradierEnvironment::Live => "Tradier",
            TradierEnvironment::Paper => "Tradier Paper",
        }
    }

    fn is_connected(&self) -> bool {
        self.connected
    }

    fn connect(&mut self) -> lean_core::Result<()> {
        match block_on_tradier(self.client.get_balance(&self.account_id)) {
            Ok(_) => {
                self.connected = true;
                info!(
                    "Tradier: connected to {} account {}",
                    self.environment.label(),
                    self.account_id
                );
                Ok(())
            }
            Err(error) => {
                self.connected = false;
                Err(lean_core::LeanError::BrokerageError(format!(
                    "Tradier connection failed for account {}: {}",
                    self.account_id, error
                )))
            }
        }
    }

    fn disconnect(&mut self) {
        self.connected = false;
        info!("Tradier: disconnected");
    }

    fn place_order(&mut self, order: Order) -> lean_core::Result<bool> {
        Ok(self.place_order_with_brokerage_ids(order)?.is_some())
    }

    fn place_order_with_brokerage_ids(
        &mut self,
        order: Order,
    ) -> lean_core::Result<Option<Vec<String>>> {
        match block_on_tradier(self.place_order_async(&order)) {
            Ok(tradier_ids) => {
                info!("Tradier: place_order succeeded, tradier_ids={tradier_ids:?}");
                Ok(Some(
                    tradier_ids
                        .into_iter()
                        .map(|tradier_id| tradier_id.to_string())
                        .collect(),
                ))
            }
            Err(e) => {
                error!("Tradier: place_order failed: {}", e);
                Ok(None)
            }
        }
    }

    fn update_order(&mut self, order: &Order) -> lean_core::Result<bool> {
        let tradier_id = match parse_brokerage_id(order) {
            Some(id) => id,
            None => {
                warn!(
                    "Tradier: update_order — no brokerage ID on order {}",
                    order.id
                );
                return Ok(false);
            }
        };

        let order_type_str = match order.order_type {
            OrderType::Market => "market",
            OrderType::Limit => "limit",
            OrderType::StopLimit => "stop_limit",
            OrderType::StopMarket => "stop",
            other => {
                warn!("Tradier: update_order — unsupported order type {:?}", other);
                return Ok(false);
            }
        };

        let price = order.limit_price.and_then(|p| p.to_f64());
        let stop = order.stop_price.and_then(|p| p.to_f64());
        let duration = match tradier_duration_for_order(order) {
            Ok(duration) => duration,
            Err(error) => {
                warn!("Tradier: update_order — {error}");
                return Ok(false);
            }
        };

        match block_on_tradier(self.modify_order_async(
            tradier_id,
            order_type_str,
            duration,
            price,
            stop,
        )) {
            Ok(()) => {
                info!(
                    "Tradier: update_order succeeded for tradier_id={}",
                    tradier_id
                );
                Ok(true)
            }
            Err(e) => {
                error!("Tradier: update_order failed: {}", e);
                Ok(false)
            }
        }
    }

    fn can_update_order(&self, _order: &Order, request: &UpdateOrderRequest) -> bool {
        request
            .fields
            .quantity
            .map(|quantity| quantity == request.previous_order.quantity)
            .unwrap_or(true)
    }

    fn cancel_order(&mut self, order: &Order) -> lean_core::Result<bool> {
        let tradier_id = match parse_brokerage_id(order) {
            Some(id) => id,
            None => {
                warn!(
                    "Tradier: cancel_order — no brokerage ID on order {}",
                    order.id
                );
                return Ok(false);
            }
        };

        match block_on_tradier(self.cancel_order_async(tradier_id)) {
            Ok(()) => {
                info!(
                    "Tradier: cancel_order succeeded for tradier_id={}",
                    tradier_id
                );
                Ok(true)
            }
            Err(e) => {
                error!("Tradier: cancel_order failed: {}", e);
                Ok(false)
            }
        }
    }

    fn get_open_orders(&self) -> Vec<Order> {
        match block_on_tradier(self.client.get_orders(&self.account_id)) {
            Ok(tradier_orders) => tradier_orders
                .into_iter()
                .filter(|o| is_open_tradier_status(&o.status))
                .filter_map(|o| lean_order_from_tradier(&o))
                .collect(),
            Err(e) => {
                error!("Tradier: get_open_orders failed: {}", e);
                Vec::new()
            }
        }
    }

    fn get_account_orders(&self) -> Vec<Order> {
        match block_on_tradier(self.client.get_orders(&self.account_id)) {
            Ok(tradier_orders) => tradier_orders
                .into_iter()
                .filter_map(|order| lean_order_from_tradier(&order))
                .collect(),
            Err(e) => {
                error!("Tradier: get_account_orders failed: {}", e);
                Vec::new()
            }
        }
    }

    fn get_cash_balance(&self) -> Vec<(String, Price)> {
        match block_on_tradier(self.fetch_cash_balance()) {
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
        match block_on_tradier(self.fetch_positions()) {
            Ok(positions) => positions
                .into_iter()
                .map(|(ticker, qty)| {
                    let symbol = parse_osi_symbol(&ticker)
                        .unwrap_or_else(|| Symbol::create_equity(&ticker, &Market::usa()));
                    let quantity = Quantity::from(qty);
                    (symbol, quantity)
                })
                .collect(),
            Err(e) => {
                error!("Tradier: get_account_holdings failed: {}", e);
                HashMap::new()
            }
        }
    }

    fn get_account_detailed_holdings(&self) -> Vec<BrokerageHolding> {
        match block_on_tradier(self.client.get_positions(&self.account_id)) {
            Ok(positions) => positions
                .into_iter()
                .filter_map(|position| {
                    let symbol = parse_osi_symbol(&position.symbol)
                        .unwrap_or_else(|| Symbol::create_equity(&position.symbol, &Market::usa()));
                    let quantity = Quantity::from(position.quantity);
                    let average_price = average_price_from_cost_basis(
                        position.cost_basis,
                        position.quantity,
                        brokerage_contract_multiplier(&symbol),
                    );
                    Some(BrokerageHolding {
                        symbol,
                        quantity,
                        average_price,
                    })
                })
                .collect(),
            Err(e) => {
                error!("Tradier: get_account_detailed_holdings failed: {}", e);
                Vec::new()
            }
        }
    }
}

// ─── Async API ────────────────────────────────────────────────────────────────

impl TradierBrokerage {
    /// Place a LEAN order via Tradier's REST API (async).
    pub async fn place_order_async(&self, order: &Order) -> Result<Vec<i64>> {
        let wire = tradier_order_symbols(&order.symbol)?;
        let current_position = self
            .current_position_quantity(&wire.order_symbol())
            .await
            .unwrap_or_else(|error| {
                warn!(
                    "Tradier: could not fetch current position for {}: {}",
                    wire.order_symbol(),
                    error
                );
                0
            });
        let legs = split_cross_zero_order(order.quantity, current_position);
        let mut tradier_ids = Vec::with_capacity(legs.len());

        for leg in legs {
            let mut leg_order = order.clone();
            leg_order.quantity = leg.quantity;
            let (direction, class, order_type_str, duration, price_str, stop_str) =
                translate_order(&leg_order, leg.current_position)?;

            let qty = leg_order.quantity.abs().to_string();
            let mut params: Vec<(&str, String)> = vec![
                ("class", class),
                ("symbol", wire.underlying_or_symbol.clone()),
                ("side", direction),
                ("quantity", qty),
                ("type", order_type_str),
                ("duration", duration),
            ];
            if let Some(option_symbol) = wire.option_symbol.clone() {
                params.push(("option_symbol", option_symbol));
            }
            if let Some(p) = price_str {
                params.push(("price", p));
            }
            if let Some(s) = stop_str {
                params.push(("stop", s));
            }

            let resp = self.client.place_order(&self.account_id, &params).await?;

            if let Some(errors) = resp.errors {
                if !errors.error.is_empty() {
                    anyhow::bail!("Tradier order rejected: {}", errors.error.join("; "));
                }
            }

            info!(
                "Tradier: placed order id={} status={}",
                resp.order.id, resp.order.status
            );
            tradier_ids.push(resp.order.id);
        }

        Ok(tradier_ids)
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
            ("type", order_type.to_string()),
            ("duration", duration.to_string()),
        ];
        if let Some(p) = price {
            params.push(("price", format!("{p:.2}")));
        }
        if let Some(s) = stop {
            params.push(("stop", format!("{s:.2}")));
        }

        let resp = self
            .client
            .modify_order(&self.account_id, tradier_order_id, &params)
            .await?;
        info!(
            "Tradier: modified order id={} status={}",
            resp.order.id, resp.order.status
        );
        Ok(())
    }

    /// Cancel an open order (async).
    pub async fn cancel_order_async(&self, tradier_order_id: i64) -> Result<()> {
        let resp = self
            .client
            .cancel_order(&self.account_id, tradier_order_id)
            .await?;
        info!(
            "Tradier: cancelled order id={} status={}",
            resp.order.id, resp.order.status
        );
        Ok(())
    }

    async fn current_position_quantity(&self, symbol: &str) -> Result<i64> {
        let positions = self.client.get_positions(&self.account_id).await?;
        Ok(positions
            .into_iter()
            .find(|position| position.symbol.eq_ignore_ascii_case(symbol))
            .map(|position| position.quantity)
            .unwrap_or_default())
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Extract the first brokerage ID (Tradier order ID) from a LEAN Order.
fn parse_brokerage_id(order: &Order) -> Option<i64> {
    order.brokerage_id.first()?.parse::<i64>().ok()
}

fn brokerage_contract_multiplier(symbol: &Symbol) -> Decimal {
    if symbol.option_symbol_id().is_some() {
        Decimal::from(100)
    } else {
        Decimal::ONE
    }
}

fn average_price_from_cost_basis(
    cost_basis: f64,
    quantity: i64,
    contract_multiplier: Decimal,
) -> Decimal {
    if quantity == 0 || contract_multiplier <= Decimal::ZERO {
        return Decimal::ZERO;
    }

    let cost_basis = Decimal::from_f64(cost_basis).unwrap_or(Decimal::ZERO).abs();
    cost_basis / Decimal::from(quantity.abs()) / contract_multiplier
}

fn block_on_tradier<F, T>(future: F) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(future),
    }
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
    let symbol = tradier_order_to_symbol(o)?;

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
        TradierOrderStatus::Filled => OrderStatus::Filled,
        TradierOrderStatus::Cancelled => OrderStatus::Canceled,
        TradierOrderStatus::Rejected => OrderStatus::Invalid,
        TradierOrderStatus::Expired => OrderStatus::Canceled,
        TradierOrderStatus::Open
        | TradierOrderStatus::PartiallyFilled
        | TradierOrderStatus::Pending
        | TradierOrderStatus::Submitted => OrderStatus::Submitted,
    };

    let now = DateTime::now();
    let mut lean_order = match o.order_type {
        TradierOrderType::Market => Order::market(o.id, symbol, signed_qty, now, ""),
        TradierOrderType::Limit => {
            let lp = Decimal::from_f64(o.price)?;
            Order::limit(o.id, symbol, signed_qty, lp, now, "")
        }
        TradierOrderType::StopMarket => {
            let sp = tradier_order_stop_price(o)?;
            Order::stop_market(o.id, symbol, signed_qty, sp, now, "")
        }
        TradierOrderType::StopLimit => {
            let lp = Decimal::from_f64(o.price)?;
            let sp = tradier_order_stop_price(o).unwrap_or(lp);
            Order::stop_limit(o.id, symbol, signed_qty, sp, lp, now, "")
        }
        _ => return None, // Credit/Debit/Even not representable in LEAN
    };

    lean_order.status = lean_status;
    lean_order.brokerage_id = vec![o.id.to_string()];
    let executed = Decimal::from_f64(o.exec_quantity).unwrap_or(Decimal::ZERO);
    lean_order.filled_quantity = if signed_qty < Decimal::ZERO {
        -executed
    } else {
        executed
    };
    lean_order.average_fill_price = tradier_order_fill_price(o);

    Some(lean_order)
}

fn tradier_order_fill_price(order: &TradierOrder) -> Decimal {
    Decimal::from_f64(order.avg_fill_price)
        .filter(|price| *price > Decimal::ZERO)
        .or_else(|| Decimal::from_f64(order.last_fill_price).filter(|price| *price > Decimal::ZERO))
        .or_else(|| Decimal::from_f64(order.price).filter(|price| *price > Decimal::ZERO))
        .unwrap_or(Decimal::ZERO)
}

fn tradier_order_stop_price(order: &TradierOrder) -> Option<Decimal> {
    Decimal::from_f64(order.stop_price)
        .filter(|price| *price > Decimal::ZERO)
        .or_else(|| Decimal::from_f64(order.price).filter(|price| *price > Decimal::ZERO))
}

// ─── Order translation ────────────────────────────────────────────────────────

type TradierOrderParams = (
    String,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
);

#[derive(Debug, Clone)]
struct TradierOrderSymbols {
    underlying_or_symbol: String,
    option_symbol: Option<String>,
}

impl TradierOrderSymbols {
    fn order_symbol(&self) -> String {
        self.option_symbol
            .clone()
            .unwrap_or_else(|| self.underlying_or_symbol.clone())
    }
}

fn tradier_order_symbols(symbol: &Symbol) -> Result<TradierOrderSymbols> {
    if matches!(
        symbol.security_type(),
        SecurityType::Option | SecurityType::IndexOption
    ) {
        let option_id = symbol
            .option_symbol_id()
            .ok_or_else(|| anyhow::anyhow!("Tradier: option symbol lacks OSI metadata"))?;
        let underlying = option_id.underlying.permtick.to_ascii_uppercase();
        let option_symbol = lean_core::format_option_ticker(
            &underlying,
            option_id.strike,
            option_id.expiry,
            option_id.right,
        );
        Ok(TradierOrderSymbols {
            underlying_or_symbol: underlying,
            option_symbol: Some(option_symbol),
        })
    } else if symbol.security_type() == SecurityType::Equity {
        Ok(TradierOrderSymbols {
            underlying_or_symbol: symbol.permtick.to_ascii_uppercase(),
            option_symbol: None,
        })
    } else {
        anyhow::bail!(
            "Tradier: unsupported security type {:?}",
            symbol.security_type()
        )
    }
}

/// Translate a LEAN order into Tradier API parameters.
///
/// Returns `(side, class, type, duration, price?, stop?)`.
fn translate_order(order: &Order, current_position: i64) -> Result<TradierOrderParams> {
    validate_order_basics(order, current_position)?;

    let is_buy = order.quantity > Decimal::ZERO;

    let (class, side) = match order.symbol.security_type() {
        SecurityType::Equity => {
            let side = if is_buy {
                if current_position < 0 {
                    "buy_to_cover"
                } else {
                    "buy"
                }
            } else if current_position <= 0 {
                "sell_short"
            } else {
                "sell"
            };
            ("equity", side)
        }
        SecurityType::Option | SecurityType::IndexOption => {
            let side = if is_buy {
                if current_position < 0 {
                    "buy_to_close"
                } else {
                    "buy_to_open"
                }
            } else if current_position > 0 {
                "sell_to_close"
            } else {
                "sell_to_open"
            };
            ("option", side)
        }
        other => anyhow::bail!("Tradier: unsupported security type {:?}", other),
    };

    let order_type = match order.order_type {
        OrderType::Market => "market",
        OrderType::Limit => "limit",
        OrderType::StopLimit => "stop_limit",
        OrderType::StopMarket => "stop",
        other => anyhow::bail!("Tradier: unsupported order type {:?}", other),
    };

    let price_str = match order.order_type {
        OrderType::Limit | OrderType::StopLimit => order.limit_price.map(|p| format!("{:.2}", p)),
        _ => None,
    };

    let stop_str = match order.order_type {
        OrderType::StopMarket | OrderType::StopLimit => {
            order.stop_price.map(|p| format!("{:.2}", p))
        }
        _ => None,
    };

    let duration = tradier_duration_for_order(order)?;

    Ok((
        side.to_string(),
        class.to_string(),
        order_type.to_string(),
        duration.to_string(),
        price_str,
        stop_str,
    ))
}

fn tradier_duration(time_in_force: &TimeInForce) -> Result<&'static str> {
    match time_in_force {
        TimeInForce::Day => Ok("day"),
        TimeInForce::GoodTilCanceled => Ok("gtc"),
        _ => anyhow::bail!("Tradier: unsupported time in force {:?}", time_in_force),
    }
}

fn tradier_duration_for_order(order: &Order) -> Result<&'static str> {
    if order.properties.outside_regular_trading_hours {
        if let Some(duration) = tradier_extended_session(order.time) {
            return Ok(duration);
        }
    }
    tradier_duration(&order.time_in_force)
}

fn tradier_extended_session(time: DateTime) -> Option<&'static str> {
    let local = time.to_tz(lean_core::time::tz::NEW_YORK);
    let seconds = local.num_seconds_from_midnight();
    match seconds {
        14_400..33_840 => Some("pre"),
        57_600..71_700 => Some("post"),
        _ => None,
    }
}

fn tradier_regular_session(time: DateTime) -> bool {
    let local = time.to_tz(lean_core::time::tz::NEW_YORK);
    matches!(local.num_seconds_from_midnight(), 34_200..57_600)
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct TradierOrderLeg {
    quantity: Decimal,
    current_position: i64,
}

fn split_cross_zero_order(quantity: Decimal, current_position: i64) -> Vec<TradierOrderLeg> {
    let current = Decimal::from(current_position);
    let projected = current + quantity;
    if current > Decimal::ZERO && quantity < Decimal::ZERO && projected < Decimal::ZERO {
        vec![
            TradierOrderLeg {
                quantity: -current,
                current_position,
            },
            TradierOrderLeg {
                quantity: projected,
                current_position: 0,
            },
        ]
    } else if current < Decimal::ZERO && quantity > Decimal::ZERO && projected > Decimal::ZERO {
        vec![
            TradierOrderLeg {
                quantity: -current,
                current_position,
            },
            TradierOrderLeg {
                quantity: projected,
                current_position: 0,
            },
        ]
    } else {
        vec![TradierOrderLeg {
            quantity,
            current_position,
        }]
    }
}

fn validate_order_basics(order: &Order, current_position: i64) -> Result<()> {
    let abs_quantity = order.quantity.abs();
    if abs_quantity < Decimal::ONE || abs_quantity > Decimal::from(10_000_000u64) {
        anyhow::bail!(
            "Tradier: order quantity must be between 1 and 10000000, got {}",
            abs_quantity
        );
    }

    let projected_position = Decimal::from(current_position) + order.quantity;
    if projected_position < Decimal::ZERO && order.time_in_force == TimeInForce::GoodTilCanceled {
        anyhow::bail!("Tradier: GTC orders cannot leave a short position");
    }

    if order.properties.outside_regular_trading_hours {
        if order.symbol.security_type() != SecurityType::Equity
            || order.order_type != OrderType::Limit
        {
            anyhow::bail!("Tradier: extended-hours orders must be equity limit orders");
        }
        if !tradier_regular_session(order.time) && tradier_extended_session(order.time).is_none() {
            anyhow::bail!(
                "Tradier: extended-hours orders must be submitted during pre-market or post-market"
            );
        }
    }

    Ok(())
}

fn tradier_order_to_symbol(order: &TradierOrder) -> Option<Symbol> {
    match order.order_class {
        super::models::TradierOrderClass::Option | super::models::TradierOrderClass::Multileg => {
            order
                .option_symbol
                .as_deref()
                .or(Some(order.symbol.as_str()))
                .and_then(parse_osi_symbol)
        }
        _ => Some(Symbol::create_equity(&order.symbol, &Market::usa())),
    }
}

fn parse_osi_symbol(value: &str) -> Option<Symbol> {
    let value = value.trim().to_ascii_uppercase();
    if value.len() < 16 {
        return None;
    }
    let tail_start = value.len().checked_sub(15)?;
    let underlying = &value[..tail_start];
    let expiry = &value[tail_start..tail_start + 6];
    let right = &value[tail_start + 6..tail_start + 7];
    let strike = &value[tail_start + 7..];

    let year = 2000 + expiry[0..2].parse::<i32>().ok()?;
    let month = expiry[2..4].parse::<u32>().ok()?;
    let day = expiry[4..6].parse::<u32>().ok()?;
    let expiry = chrono::NaiveDate::from_ymd_opt(year, month, day)?;
    let right = match right {
        "C" => OptionRight::Call,
        "P" => OptionRight::Put,
        _ => return None,
    };
    let strike = Decimal::from_i64(strike.parse::<i64>().ok()?)? / Decimal::from(1000);
    if is_supported_index_option_root(underlying) {
        let underlying_symbol = Symbol::create_index(underlying, &Market::usa());
        Some(Symbol::create_index_option_osi(
            underlying_symbol,
            strike,
            expiry,
            right,
            OptionStyle::European,
            &Market::usa(),
        ))
    } else {
        let underlying_symbol = Symbol::create_equity(underlying, &Market::usa());
        Some(Symbol::create_option_osi(
            underlying_symbol,
            strike,
            expiry,
            right,
            OptionStyle::American,
            &Market::usa(),
        ))
    }
}

fn is_supported_index_option_root(root: &str) -> bool {
    matches!(
        root.to_ascii_uppercase().as_str(),
        "SPX" | "NDX" | "VIX" | "RUT" | "RUTW" | "SPXW" | "VIXW" | "NDXP" | "NQX"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{TradierOrderClass, TradierOrderDuration};
    use chrono::TimeZone;
    use lean_core::{DateTime, Market};
    use rust_decimal_macros::dec;

    fn equity_order(quantity: Decimal) -> Order {
        let mut order = Order::market(
            1,
            Symbol::create_equity("SPY", &Market::usa()),
            quantity,
            DateTime::EPOCH,
            "",
        );
        order.time_in_force = TimeInForce::Day;
        order
    }

    fn option_symbol() -> Symbol {
        let underlying = Symbol::create_equity("SPY", &Market::usa());
        Symbol::create_option_osi(
            underlying,
            dec!(450),
            chrono::NaiveDate::from_ymd_opt(2026, 1, 16).unwrap(),
            OptionRight::Call,
            OptionStyle::American,
            &Market::usa(),
        )
    }

    fn option_order(quantity: Decimal) -> Order {
        let mut order = Order::limit(
            1,
            option_symbol(),
            quantity,
            dec!(1.25),
            DateTime::EPOCH,
            "",
        );
        order.time_in_force = TimeInForce::Day;
        order
    }

    #[test]
    fn equity_order_sides_follow_position() {
        assert_eq!(
            translate_order(&equity_order(dec!(10)), 0).unwrap().0,
            "buy"
        );
        assert_eq!(
            translate_order(&equity_order(dec!(10)), -5).unwrap().0,
            "buy_to_cover"
        );
        assert_eq!(
            translate_order(&equity_order(dec!(-5)), 10).unwrap().0,
            "sell"
        );
        assert_eq!(
            translate_order(&equity_order(dec!(-5)), 0).unwrap().0,
            "sell_short"
        );
    }

    #[test]
    fn option_order_sides_follow_position() {
        assert_eq!(
            translate_order(&option_order(dec!(1)), 0).unwrap().0,
            "buy_to_open"
        );
        assert_eq!(
            translate_order(&option_order(dec!(1)), -2).unwrap().0,
            "buy_to_close"
        );
        assert_eq!(
            translate_order(&option_order(dec!(-1)), 3).unwrap().0,
            "sell_to_close"
        );
        assert_eq!(
            translate_order(&option_order(dec!(-1)), 0).unwrap().0,
            "sell_to_open"
        );
    }

    #[test]
    fn cross_zero_orders_are_split_into_close_and_open_legs() {
        assert_eq!(
            split_cross_zero_order(dec!(-15), 10),
            vec![
                TradierOrderLeg {
                    quantity: dec!(-10),
                    current_position: 10,
                },
                TradierOrderLeg {
                    quantity: dec!(-5),
                    current_position: 0,
                },
            ]
        );
        assert_eq!(
            split_cross_zero_order(dec!(15), -10),
            vec![
                TradierOrderLeg {
                    quantity: dec!(10),
                    current_position: -10,
                },
                TradierOrderLeg {
                    quantity: dec!(5),
                    current_position: 0,
                },
            ]
        );
    }

    #[test]
    fn rejects_gtc_orders_that_leave_short_position() {
        let order = Order::market(
            1,
            Symbol::create_equity("SPY", &Market::usa()),
            dec!(-1),
            DateTime::EPOCH,
            "",
        );

        let error = translate_order(&order, 0).unwrap_err().to_string();

        assert!(error.contains("GTC"));
    }

    #[test]
    fn tradier_duration_preserves_day_and_gtc() {
        assert_eq!(tradier_duration(&TimeInForce::Day).unwrap(), "day");
        assert_eq!(
            tradier_duration(&TimeInForce::GoodTilCanceled).unwrap(),
            "gtc"
        );
    }

    #[test]
    fn tradier_rejects_quantity_updates() {
        let brokerage = TradierBrokerage::new(
            "token".to_string(),
            "account".to_string(),
            TradierEnvironment::Paper,
        );
        let previous_order = Order::limit(
            1,
            Symbol::create_equity("SPY", &Market::usa()),
            dec!(1),
            dec!(450),
            DateTime::EPOCH,
            "",
        );
        let mut updated_order = previous_order.clone();
        updated_order.limit_price = Some(dec!(451));

        let price_request = UpdateOrderRequest {
            order_id: previous_order.id,
            time: DateTime::EPOCH,
            fields: lean_orders::UpdateOrderFields {
                limit_price: Some(dec!(451)),
                ..Default::default()
            },
            previous_order: previous_order.clone(),
        };
        assert!(brokerage.can_update_order(&updated_order, &price_request));

        let unchanged_quantity_request = UpdateOrderRequest {
            fields: lean_orders::UpdateOrderFields {
                quantity: Some(dec!(1)),
                ..Default::default()
            },
            ..price_request.clone()
        };
        assert!(brokerage.can_update_order(&updated_order, &unchanged_quantity_request));

        let quantity_request = UpdateOrderRequest {
            fields: lean_orders::UpdateOrderFields {
                quantity: Some(dec!(2)),
                ..Default::default()
            },
            ..price_request
        };
        assert!(!brokerage.can_update_order(&updated_order, &quantity_request));
    }

    #[test]
    fn extended_hours_equity_limit_orders_use_pre_and_post_durations() {
        let mut pre = Order::limit(
            1,
            Symbol::create_equity("SPY", &Market::usa()),
            dec!(1),
            dec!(450),
            ny_time(2026, 1, 16, 8, 0),
            "",
        );
        pre.time_in_force = TimeInForce::Day;
        pre.properties.outside_regular_trading_hours = true;
        assert_eq!(translate_order(&pre, 0).unwrap().3, "pre");

        let mut post = pre.clone();
        post.time = ny_time(2026, 1, 16, 17, 0);
        assert_eq!(translate_order(&post, 0).unwrap().3, "post");
    }

    #[test]
    fn extended_hours_flag_does_not_override_regular_session_duration() {
        let mut order = Order::limit(
            1,
            Symbol::create_equity("SPY", &Market::usa()),
            dec!(1),
            dec!(450),
            ny_time(2026, 1, 16, 10, 0),
            "",
        );
        order.time_in_force = TimeInForce::Day;
        order.properties.outside_regular_trading_hours = true;

        assert_eq!(translate_order(&order, 0).unwrap().3, "day");
    }

    #[test]
    fn extended_hours_rejects_non_limit_orders() {
        let mut order = equity_order(dec!(1));
        order.time = ny_time(2026, 1, 16, 8, 0);
        order.properties.outside_regular_trading_hours = true;

        let error = translate_order(&order, 0).unwrap_err().to_string();

        assert!(error.contains("equity limit"));
    }

    #[test]
    fn formats_tradier_option_order_symbols() {
        let wire = tradier_order_symbols(&option_symbol()).unwrap();

        assert_eq!(wire.underlying_or_symbol, "SPY");
        assert_eq!(wire.option_symbol.as_deref(), Some("SPY260116C00450000"));
    }

    #[test]
    fn parse_osi_symbol_keeps_equity_options_as_options() {
        let symbol = parse_osi_symbol("SPY260116C00450000").unwrap();

        assert_eq!(symbol.security_type(), SecurityType::Option);
        assert_eq!(
            symbol.underlying.as_ref().unwrap().security_type(),
            SecurityType::Equity
        );
        assert_eq!(symbol.value, "SPY260116C00450000");
    }

    #[test]
    fn parse_osi_symbol_detects_supported_index_options() {
        let symbol = parse_osi_symbol("SPX260116P04500000").unwrap();

        assert_eq!(symbol.security_type(), SecurityType::IndexOption);
        assert_eq!(
            symbol.underlying.as_ref().unwrap().security_type(),
            SecurityType::Index
        );
        assert_eq!(
            symbol.option_symbol_id().unwrap().style,
            OptionStyle::European
        );
        assert_eq!(symbol.value, "SPX260116P04500000");
    }

    #[test]
    fn average_price_from_cost_basis_uses_contract_multiplier() {
        assert_eq!(
            average_price_from_cost_basis(4000.0, 10, Decimal::ONE),
            dec!(400)
        );
        assert_eq!(
            average_price_from_cost_basis(-4000.0, -10, Decimal::ONE),
            dec!(400)
        );
        assert_eq!(
            average_price_from_cost_basis(250.0, 2, Decimal::from(100)),
            dec!(1.25)
        );
    }

    #[test]
    fn tradier_snapshot_order_uses_signed_filled_quantity_and_fill_price() {
        let order = TradierOrder {
            id: 123,
            order_type: TradierOrderType::Market,
            symbol: "SPY".to_string(),
            option_symbol: None,
            side: TradierOrderDirection::Sell,
            quantity: 10.0,
            status: TradierOrderStatus::Filled,
            duration: TradierOrderDuration::Day,
            price: 0.0,
            stop_price: 0.0,
            avg_fill_price: 0.0,
            exec_quantity: 10.0,
            last_fill_price: 450.25,
            last_fill_quantity: 10.0,
            remaining_quantity: 0.0,
            create_date: "2026-01-01T14:30:00Z".to_string(),
            transaction_date: "2026-01-01T14:31:00Z".to_string(),
            order_class: TradierOrderClass::Equity,
            reason_description: None,
        };

        let lean_order = lean_order_from_tradier(&order).unwrap();

        assert_eq!(lean_order.status, OrderStatus::Filled);
        assert_eq!(lean_order.quantity, dec!(-10));
        assert_eq!(lean_order.filled_quantity, dec!(-10));
        assert_eq!(lean_order.average_fill_price, dec!(450.25));
        assert_eq!(lean_order.brokerage_id, vec!["123".to_string()]);
    }

    #[test]
    fn tradier_snapshot_stop_limit_preserves_stop_price() {
        let order: TradierOrder = serde_json::from_str(
            r#"{
                "id": 124,
                "type": "stop_limit",
                "symbol": "SPY",
                "side": "buy",
                "quantity": 10,
                "status": "open",
                "duration": "day",
                "price": 451.25,
                "stop_price": 450.50,
                "create_date": "2026-01-01T14:30:00Z",
                "transaction_date": "2026-01-01T14:31:00Z",
                "class": "equity"
            }"#,
        )
        .unwrap();

        let lean_order = lean_order_from_tradier(&order).unwrap();

        assert_eq!(lean_order.order_type, OrderType::StopLimit);
        assert_eq!(lean_order.limit_price, Some(dec!(451.25)));
        assert_eq!(lean_order.stop_price, Some(dec!(450.50)));
    }

    fn ny_time(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> DateTime {
        let local = lean_core::time::tz::NEW_YORK
            .with_ymd_and_hms(year, month, day, hour, minute, 0)
            .unwrap();
        local.with_timezone(&chrono::Utc).into()
    }
}
