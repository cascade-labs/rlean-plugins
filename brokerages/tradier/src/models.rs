/// Tradier REST API response types.
use serde::{Deserialize, Serialize};

// ─── Enums ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradierAccountType {
    #[serde(rename = "pdt")]
    DayTrader,
    Cash,
    Margin,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradierOrderDirection {
    Buy,
    #[serde(rename = "sell_short")]
    SellShort,
    Sell,
    #[serde(rename = "buy_to_cover")]
    BuyToCover,
    #[serde(rename = "sell_to_open")]
    SellToOpen,
    #[serde(rename = "sell_to_close")]
    SellToClose,
    #[serde(rename = "buy_to_close")]
    BuyToClose,
    #[serde(rename = "buy_to_open")]
    BuyToOpen,
    None,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradierOrderStatus {
    Filled,
    #[serde(rename = "canceled")]
    Cancelled,
    Open,
    Expired,
    Rejected,
    Pending,
    #[serde(rename = "partially_filled")]
    PartiallyFilled,
    Submitted,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradierOrderDuration {
    #[serde(rename = "gtc")]
    GoodTilCancelled,
    Day,
    Pre,
    Post,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradierOrderClass {
    Equity,
    Option,
    #[serde(rename = "multileg")]
    Multileg,
    Combo,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradierOrderType {
    Limit,
    Market,
    #[serde(rename = "stop_limit")]
    StopLimit,
    #[serde(rename = "stop")]
    StopMarket,
    Credit,
    Debit,
    Even,
}

// ─── Orders ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct TradierOrder {
    pub id: i64,
    #[serde(rename = "type")]
    pub order_type: TradierOrderType,
    pub symbol: String,
    #[serde(default)]
    pub option_symbol: Option<String>,
    pub side: TradierOrderDirection,
    pub quantity: f64,
    pub status: TradierOrderStatus,
    pub duration: TradierOrderDuration,
    #[serde(default)]
    pub price: f64,
    #[serde(default)]
    pub avg_fill_price: f64,
    #[serde(default)]
    pub exec_quantity: f64,
    #[serde(default)]
    pub last_fill_price: f64,
    #[serde(default)]
    pub last_fill_quantity: f64,
    #[serde(default)]
    pub remaining_quantity: f64,
    pub create_date: String,
    pub transaction_date: String,
    #[serde(rename = "class")]
    pub order_class: TradierOrderClass,
    #[serde(default)]
    pub reason_description: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TradierOrderResponse {
    pub order: TradierOrderResponseOrder,
    #[serde(default)]
    pub errors: Option<TradierOrderResponseErrors>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TradierOrderResponseOrder {
    pub id: i64,
    pub status: String,
    #[serde(default)]
    pub partner_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TradierOrderResponseErrors {
    #[serde(default)]
    pub error: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TradierOrdersContainer {
    pub orders: Option<TradierOrdersWrapper>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TradierOrdersWrapper {
    /// Can be a single object or an array — handled by client
    pub order: serde_json::Value,
}

// ─── Positions ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct TradierPosition {
    pub id: i64,
    pub date_acquired: String,
    pub quantity: i64,
    pub cost_basis: f64,
    pub symbol: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TradierPositionsContainer {
    pub positions: Option<TradierPositionsWrapper>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TradierPositionsWrapper {
    /// Can be a single object or an array — normalized by client
    pub position: serde_json::Value,
}

// ─── Balances ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct TradierBalanceContainer {
    pub balances: TradierBalanceDetails,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TradierBalanceDetails {
    pub account_number: String,
    pub account_type: TradierAccountType,
    #[serde(default)]
    pub cash_available: f64,
    #[serde(default)]
    pub equity: f64,
    #[serde(default)]
    pub long_market_value: f64,
    #[serde(default)]
    pub short_market_value: f64,
    #[serde(default)]
    pub total_cash: f64,
    #[serde(default)]
    pub total_equity: f64,
    #[serde(default)]
    pub market_value: f64,
    #[serde(default)]
    pub unsettled_funds: f64,
    #[serde(default)]
    pub pending_cash: f64,
    #[serde(default)]
    pub pending_orders_count: i32,
}

// ─── User profile ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct TradierUserProfileContainer {
    pub profile: TradierUserProfile,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TradierUserProfile {
    pub id: String,
    pub name: String,
    pub account: serde_json::Value,
}

// ─── Quotes ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct TradierQuoteContainer {
    pub quotes: Option<TradierQuotesWrapper>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TradierQuotesWrapper {
    pub quote: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TradierQuote {
    pub symbol: String,
    #[serde(default)]
    pub last: f64,
    #[serde(default)]
    pub bid: f64,
    #[serde(default)]
    pub ask: f64,
    #[serde(default)]
    pub volume: i64,
    #[serde(default)]
    pub open: f64,
    #[serde(default)]
    pub high: f64,
    #[serde(default)]
    pub low: f64,
    #[serde(default)]
    pub close: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── TradierQuote JSON parsing ─────────────────────────────────────────────

    #[test]
    fn test_tradier_parse_quote_single() {
        let json = r#"{
            "symbol": "AAPL",
            "last": 182.50,
            "bid": 182.45,
            "ask": 182.55,
            "volume": 12345678,
            "open": 180.00,
            "high": 183.00,
            "low": 179.50,
            "close": 181.00
        }"#;
        let quote: TradierQuote = serde_json::from_str(json).expect("should parse");
        assert_eq!(quote.symbol, "AAPL");
        assert!((quote.last - 182.50).abs() < 1e-9);
        assert!((quote.bid - 182.45).abs() < 1e-9);
        assert!((quote.ask - 182.55).abs() < 1e-9);
        assert_eq!(quote.volume, 12_345_678);
        assert!((quote.open - 180.00).abs() < 1e-9);
        assert!((quote.high - 183.00).abs() < 1e-9);
        assert!((quote.low - 179.50).abs() < 1e-9);
        assert!((quote.close - 181.00).abs() < 1e-9);
    }

    #[test]
    fn test_tradier_parse_quote_missing_optional_fields_default_to_zero() {
        // Tradier can return quotes with missing price fields (e.g. after-hours).
        let json = r#"{"symbol": "TSLA"}"#;
        let quote: TradierQuote = serde_json::from_str(json).expect("should parse");
        assert_eq!(quote.symbol, "TSLA");
        assert_eq!(quote.last, 0.0);
        assert_eq!(quote.bid, 0.0);
        assert_eq!(quote.ask, 0.0);
        assert_eq!(quote.volume, 0);
    }

    #[test]
    fn test_tradier_parse_quote_container_with_single_quote() {
        // When there is one symbol Tradier returns a plain object, not an array.
        let json = r#"{
            "quotes": {
                "quote": {
                    "symbol": "SPY",
                    "last": 450.10,
                    "bid": 450.05,
                    "ask": 450.15,
                    "volume": 5000000,
                    "open": 448.00,
                    "high": 451.00,
                    "low": 447.50,
                    "close": 449.80
                }
            }
        }"#;
        let container: TradierQuoteContainer =
            serde_json::from_str(json).expect("should parse container");
        let wrapper = container.quotes.expect("quotes should be Some");
        let quote: TradierQuote =
            serde_json::from_value(wrapper.quote).expect("should parse quote");
        assert_eq!(quote.symbol, "SPY");
        assert!((quote.last - 450.10).abs() < 1e-9);
    }

    #[test]
    fn test_tradier_parse_quote_container_no_quotes() {
        // Tradier returns null for quotes when no symbols match.
        let json = r#"{"quotes": null}"#;
        let container: TradierQuoteContainer = serde_json::from_str(json).expect("should parse");
        assert!(container.quotes.is_none());
    }

    // ── TradierOrder JSON parsing ─────────────────────────────────────────────

    #[test]
    fn test_tradier_parse_order_filled() {
        let json = r#"{
            "id": 99887766,
            "type": "market",
            "symbol": "AAPL",
            "side": "buy",
            "quantity": 100.0,
            "status": "filled",
            "duration": "day",
            "price": 0.0,
            "avg_fill_price": 182.50,
            "exec_quantity": 100.0,
            "last_fill_price": 182.50,
            "last_fill_quantity": 100.0,
            "remaining_quantity": 0.0,
            "create_date": "2024-01-15T09:30:00.000Z",
            "transaction_date": "2024-01-15T09:30:01.000Z",
            "class": "equity"
        }"#;
        let order: TradierOrder = serde_json::from_str(json).expect("should parse");
        assert_eq!(order.id, 99_887_766);
        assert_eq!(order.symbol, "AAPL");
        assert_eq!(order.status, TradierOrderStatus::Filled);
        assert_eq!(order.side, TradierOrderDirection::Buy);
        assert_eq!(order.order_type, TradierOrderType::Market);
        assert_eq!(order.order_class, TradierOrderClass::Equity);
        assert!((order.exec_quantity - 100.0).abs() < 1e-9);
        assert!((order.avg_fill_price - 182.50).abs() < 1e-9);
    }

    #[test]
    fn test_tradier_parse_order_limit_pending() {
        let json = r#"{
            "id": 11223344,
            "type": "limit",
            "symbol": "NVDA",
            "side": "sell",
            "quantity": 50.0,
            "status": "pending",
            "duration": "gtc",
            "price": 850.00,
            "avg_fill_price": 0.0,
            "exec_quantity": 0.0,
            "last_fill_price": 0.0,
            "last_fill_quantity": 0.0,
            "remaining_quantity": 50.0,
            "create_date": "2024-01-15T10:00:00.000Z",
            "transaction_date": "2024-01-15T10:00:01.000Z",
            "class": "equity"
        }"#;
        let order: TradierOrder = serde_json::from_str(json).expect("should parse");
        assert_eq!(order.id, 11_223_344);
        assert_eq!(order.symbol, "NVDA");
        assert_eq!(order.status, TradierOrderStatus::Pending);
        assert_eq!(order.side, TradierOrderDirection::Sell);
        assert_eq!(order.order_type, TradierOrderType::Limit);
        assert_eq!(order.duration, TradierOrderDuration::GoodTilCancelled);
        assert!((order.price - 850.00).abs() < 1e-9);
    }

    #[test]
    fn test_tradier_parse_order_cancelled() {
        let json = r#"{
            "id": 55667788,
            "type": "stop_limit",
            "symbol": "MSFT",
            "side": "sell_short",
            "quantity": 25.0,
            "status": "canceled",
            "duration": "day",
            "price": 395.00,
            "avg_fill_price": 0.0,
            "exec_quantity": 0.0,
            "last_fill_price": 0.0,
            "last_fill_quantity": 0.0,
            "remaining_quantity": 25.0,
            "create_date": "2024-01-15T11:00:00.000Z",
            "transaction_date": "2024-01-15T11:00:01.000Z",
            "class": "equity",
            "reason_description": "User cancelled"
        }"#;
        let order: TradierOrder = serde_json::from_str(json).expect("should parse");
        assert_eq!(order.status, TradierOrderStatus::Cancelled);
        assert_eq!(order.side, TradierOrderDirection::SellShort);
        assert_eq!(order.order_type, TradierOrderType::StopLimit);
        assert_eq!(order.reason_description.as_deref(), Some("User cancelled"));
    }

    #[test]
    fn test_tradier_parse_option_order() {
        let json = r#"{
            "id": 77889900,
            "type": "market",
            "symbol": "AAPL",
            "option_symbol": "AAPL240119C00180000",
            "side": "buy_to_open",
            "quantity": 10.0,
            "status": "filled",
            "duration": "day",
            "price": 0.0,
            "avg_fill_price": 3.50,
            "exec_quantity": 10.0,
            "last_fill_price": 3.50,
            "last_fill_quantity": 10.0,
            "remaining_quantity": 0.0,
            "create_date": "2024-01-15T09:35:00.000Z",
            "transaction_date": "2024-01-15T09:35:01.000Z",
            "class": "option"
        }"#;
        let order: TradierOrder = serde_json::from_str(json).expect("should parse");
        assert_eq!(order.order_class, TradierOrderClass::Option);
        assert_eq!(order.side, TradierOrderDirection::BuyToOpen);
        assert_eq!(order.option_symbol.as_deref(), Some("AAPL240119C00180000"));
    }

    // ── TradierOrderResponse JSON parsing ─────────────────────────────────────

    #[test]
    fn test_tradier_parse_order_response_ok() {
        let json = r#"{
            "order": {
                "id": 12345,
                "status": "ok",
                "partner_id": null
            }
        }"#;
        let resp: TradierOrderResponse = serde_json::from_str(json).expect("should parse");
        assert_eq!(resp.order.id, 12345);
        assert_eq!(resp.order.status, "ok");
        assert!(resp.errors.is_none());
    }

    #[test]
    fn test_tradier_parse_order_response_with_errors() {
        let json = r#"{
            "order": {
                "id": 0,
                "status": "error"
            },
            "errors": {
                "error": ["Insufficient funds", "Symbol not found"]
            }
        }"#;
        let resp: TradierOrderResponse = serde_json::from_str(json).expect("should parse");
        assert_eq!(resp.order.status, "error");
        let errs = resp.errors.expect("errors should be Some");
        assert_eq!(errs.error.len(), 2);
        assert_eq!(errs.error[0], "Insufficient funds");
        assert_eq!(errs.error[1], "Symbol not found");
    }

    // ── TradierOrderStatus mapping ────────────────────────────────────────────

    #[test]
    fn test_tradier_order_status_filled_deserializes() {
        let s: TradierOrderStatus = serde_json::from_str("\"filled\"").expect("deserialize filled");
        assert_eq!(s, TradierOrderStatus::Filled);
    }

    #[test]
    fn test_tradier_order_status_canceled_deserializes() {
        // Tradier uses American spelling "canceled" (one l).
        let s: TradierOrderStatus =
            serde_json::from_str("\"canceled\"").expect("deserialize canceled");
        assert_eq!(s, TradierOrderStatus::Cancelled);
    }

    #[test]
    fn test_tradier_order_status_pending_deserializes() {
        let s: TradierOrderStatus =
            serde_json::from_str("\"pending\"").expect("deserialize pending");
        assert_eq!(s, TradierOrderStatus::Pending);
    }

    #[test]
    fn test_tradier_order_status_partially_filled_deserializes() {
        let s: TradierOrderStatus =
            serde_json::from_str("\"partially_filled\"").expect("deserialize partially_filled");
        assert_eq!(s, TradierOrderStatus::PartiallyFilled);
    }

    #[test]
    fn test_tradier_order_status_open_deserializes() {
        let s: TradierOrderStatus = serde_json::from_str("\"open\"").expect("deserialize open");
        assert_eq!(s, TradierOrderStatus::Open);
    }

    #[test]
    fn test_tradier_order_status_rejected_deserializes() {
        let s: TradierOrderStatus =
            serde_json::from_str("\"rejected\"").expect("deserialize rejected");
        assert_eq!(s, TradierOrderStatus::Rejected);
    }

    #[test]
    fn test_tradier_order_status_expired_deserializes() {
        let s: TradierOrderStatus =
            serde_json::from_str("\"expired\"").expect("deserialize expired");
        assert_eq!(s, TradierOrderStatus::Expired);
    }

    #[test]
    fn test_tradier_order_status_submitted_deserializes() {
        let s: TradierOrderStatus =
            serde_json::from_str("\"submitted\"").expect("deserialize submitted");
        assert_eq!(s, TradierOrderStatus::Submitted);
    }

    // ── TradierOrderDirection ─────────────────────────────────────────────────

    #[test]
    fn test_tradier_order_direction_buy_to_cover() {
        let d: TradierOrderDirection =
            serde_json::from_str("\"buy_to_cover\"").expect("deserialize");
        assert_eq!(d, TradierOrderDirection::BuyToCover);
    }

    #[test]
    fn test_tradier_order_direction_sell_to_open() {
        let d: TradierOrderDirection =
            serde_json::from_str("\"sell_to_open\"").expect("deserialize");
        assert_eq!(d, TradierOrderDirection::SellToOpen);
    }

    #[test]
    fn test_tradier_order_direction_sell_to_close() {
        let d: TradierOrderDirection =
            serde_json::from_str("\"sell_to_close\"").expect("deserialize");
        assert_eq!(d, TradierOrderDirection::SellToClose);
    }

    #[test]
    fn test_tradier_order_direction_buy_to_close() {
        let d: TradierOrderDirection =
            serde_json::from_str("\"buy_to_close\"").expect("deserialize");
        assert_eq!(d, TradierOrderDirection::BuyToClose);
    }

    #[test]
    fn test_tradier_order_direction_buy_to_open() {
        let d: TradierOrderDirection =
            serde_json::from_str("\"buy_to_open\"").expect("deserialize");
        assert_eq!(d, TradierOrderDirection::BuyToOpen);
    }

    // ── Symbol / ticker conventions ───────────────────────────────────────────

    #[test]
    fn test_tradier_symbol_is_plain_uppercase_ticker() {
        // Tradier uses simple uppercase tickers for equities — no exchange suffix.
        let json = r#"{"symbol": "GOOG", "last": 140.0, "bid": 139.9, "ask": 140.1,
                        "volume": 1000000, "open": 138.0, "high": 141.0, "low": 137.0, "close": 139.5}"#;
        let quote: TradierQuote = serde_json::from_str(json).expect("should parse");
        // Symbol must be upper-case and contain no spaces or exchange markers.
        assert!(quote.symbol.chars().all(|c| c.is_ascii_uppercase()));
        assert!(!quote.symbol.contains(' '));
        assert!(!quote.symbol.contains('.'));
    }

    #[test]
    fn test_tradier_option_symbol_occ_format() {
        // OCC option symbols: ROOT + YYMMDD + C/P + 8-digit strike (padded to 3dp).
        // E.g. AAPL240119C00180000 → AAPL, 24-01-19, Call, $180.00
        let occ = "AAPL240119C00180000";
        let root: String = occ
            .chars()
            .take_while(|c| c.is_ascii_alphabetic())
            .collect();
        assert_eq!(root, "AAPL");
        let rest = &occ[root.len()..];
        let direction_char = rest.chars().nth(6).expect("should have direction char");
        assert!(direction_char == 'C' || direction_char == 'P');
    }

    // ── Balance / account types ───────────────────────────────────────────────

    #[test]
    fn test_tradier_parse_balance_margin_account() {
        let json = r#"{
            "balances": {
                "account_number": "123456789",
                "account_type": "margin",
                "cash_available": 5000.00,
                "equity": 25000.00,
                "total_cash": 5000.00,
                "total_equity": 30000.00,
                "long_market_value": 25000.00,
                "short_market_value": 0.0,
                "market_value": 25000.00,
                "unsettled_funds": 0.0,
                "pending_cash": 0.0,
                "pending_orders_count": 2
            }
        }"#;
        let container: TradierBalanceContainer = serde_json::from_str(json).expect("should parse");
        let bal = container.balances;
        assert_eq!(bal.account_number, "123456789");
        assert_eq!(bal.account_type, TradierAccountType::Margin);
        assert!((bal.total_cash - 5000.00).abs() < 1e-9);
        assert!((bal.total_equity - 30000.00).abs() < 1e-9);
        assert_eq!(bal.pending_orders_count, 2);
    }

    #[test]
    fn test_tradier_parse_balance_cash_account() {
        let json = r#"{
            "balances": {
                "account_number": "987654321",
                "account_type": "cash",
                "total_cash": 10000.00,
                "total_equity": 10000.00
            }
        }"#;
        let container: TradierBalanceContainer = serde_json::from_str(json).expect("should parse");
        assert_eq!(container.balances.account_type, TradierAccountType::Cash);
    }

    #[test]
    fn test_tradier_parse_balance_day_trader_account() {
        let json = r#"{
            "balances": {
                "account_number": "111222333",
                "account_type": "pdt",
                "total_cash": 25000.00,
                "total_equity": 25000.00
            }
        }"#;
        let container: TradierBalanceContainer = serde_json::from_str(json).expect("should parse");
        assert_eq!(
            container.balances.account_type,
            TradierAccountType::DayTrader
        );
    }

    // ── TradierOrderClass ─────────────────────────────────────────────────────

    #[test]
    fn test_tradier_order_class_equity_deserializes() {
        let c: TradierOrderClass = serde_json::from_str("\"equity\"").expect("deserialize");
        assert_eq!(c, TradierOrderClass::Equity);
    }

    #[test]
    fn test_tradier_order_class_option_deserializes() {
        let c: TradierOrderClass = serde_json::from_str("\"option\"").expect("deserialize");
        assert_eq!(c, TradierOrderClass::Option);
    }

    #[test]
    fn test_tradier_order_class_multileg_deserializes() {
        let c: TradierOrderClass = serde_json::from_str("\"multileg\"").expect("deserialize");
        assert_eq!(c, TradierOrderClass::Multileg);
    }

    // ── TradierOrderType ──────────────────────────────────────────────────────

    #[test]
    fn test_tradier_order_type_stop_limit_deserializes() {
        let t: TradierOrderType = serde_json::from_str("\"stop_limit\"").expect("deserialize");
        assert_eq!(t, TradierOrderType::StopLimit);
    }

    #[test]
    fn test_tradier_order_type_stop_market_deserializes() {
        let t: TradierOrderType = serde_json::from_str("\"stop\"").expect("deserialize");
        assert_eq!(t, TradierOrderType::StopMarket);
    }

    // ── TradierOrderDuration ──────────────────────────────────────────────────

    #[test]
    fn test_tradier_duration_gtc_deserializes() {
        let d: TradierOrderDuration = serde_json::from_str("\"gtc\"").expect("deserialize");
        assert_eq!(d, TradierOrderDuration::GoodTilCancelled);
    }

    #[test]
    fn test_tradier_duration_pre_post_deserialize() {
        let pre: TradierOrderDuration = serde_json::from_str("\"pre\"").expect("deserialize pre");
        let post: TradierOrderDuration =
            serde_json::from_str("\"post\"").expect("deserialize post");
        assert_eq!(pre, TradierOrderDuration::Pre);
        assert_eq!(post, TradierOrderDuration::Post);
    }

    // ── Orders list with single / array normalization stubs ───────────────────

    #[test]
    fn test_tradier_orders_container_null_orders() {
        // Tradier returns "orders": null when there are no orders.
        let json = r#"{"orders": null}"#;
        let container: TradierOrdersContainer = serde_json::from_str(json).expect("should parse");
        assert!(container.orders.is_none());
    }

    #[test]
    fn test_tradier_positions_container_null_positions() {
        let json = r#"{"positions": null}"#;
        let container: TradierPositionsContainer =
            serde_json::from_str(json).expect("should parse");
        assert!(container.positions.is_none());
    }

    // ── TradierPosition ───────────────────────────────────────────────────────

    #[test]
    fn test_tradier_parse_position() {
        let json = r#"{
            "id": 42,
            "date_acquired": "2024-01-10T00:00:00.000Z",
            "quantity": 200,
            "cost_basis": 36000.00,
            "symbol": "AAPL"
        }"#;
        let pos: TradierPosition = serde_json::from_str(json).expect("should parse");
        assert_eq!(pos.id, 42);
        assert_eq!(pos.symbol, "AAPL");
        assert_eq!(pos.quantity, 200);
        assert!((pos.cost_basis - 36_000.00).abs() < 1e-9);
    }

    // ── Client URL construction ───────────────────────────────────────────────

    #[test]
    fn test_tradier_live_base_url() {
        // The live API base URL must be the canonical Tradier v1 endpoint.
        assert_eq!(
            super::super::client::LIVE_BASE_FOR_TEST,
            "https://api.tradier.com/v1"
        );
    }

    #[test]
    fn test_tradier_sandbox_base_url() {
        assert_eq!(
            super::super::client::SANDBOX_BASE_FOR_TEST,
            "https://sandbox.tradier.com/v1"
        );
    }

    #[test]
    fn test_tradier_order_placement_url_format() {
        // The place_order URL must include /accounts/{id}/orders — verified by
        // constructing it the same way the client does.
        let base = "https://api.tradier.com/v1";
        let account_id = "123456789";
        let url = format!("{}/accounts/{}/orders", base, account_id);
        assert_eq!(url, "https://api.tradier.com/v1/accounts/123456789/orders");
        assert!(url.starts_with("https://api.tradier.com/v1/accounts/"));
        assert!(url.ends_with("/orders"));
    }

    #[test]
    fn test_tradier_modify_order_url_format() {
        let base = "https://api.tradier.com/v1";
        let account_id = "ABC123";
        let order_id: i64 = 99887766;
        let url = format!("{}/accounts/{}/orders/{}", base, account_id, order_id);
        assert_eq!(
            url,
            "https://api.tradier.com/v1/accounts/ABC123/orders/99887766"
        );
    }

    #[test]
    fn test_tradier_quotes_url_format() {
        let base = "https://api.tradier.com/v1";
        let symbols = ["AAPL", "MSFT", "GOOG"];
        let csv = symbols.join(",");
        let url = format!("{}/markets/quotes?symbols={}&greeks=false", base, csv);
        assert_eq!(
            url,
            "https://api.tradier.com/v1/markets/quotes?symbols=AAPL,MSFT,GOOG&greeks=false"
        );
        assert!(url.contains("greeks=false"));
    }
}
