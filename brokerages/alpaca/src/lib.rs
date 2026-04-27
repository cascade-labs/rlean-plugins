/// Alpaca brokerage model.
///
/// Alpaca offers commission-free equity trading with RegT margin.
/// Intraday leverage is 4× and overnight leverage is 2× for equities.
/// Crypto is traded 24/7 at 1× (spot only, no margin).
///
/// Pattern Day Trader (PDT) rules apply: accounts under $25,000 are
/// limited to 3 day trades per 5 rolling business days.
///
/// Supported order types:
///   - Equity: Market, Limit, StopMarket, StopLimit, TrailingStop,
///             MarketOnOpen, MarketOnClose
///   - Crypto:  Market, Limit, StopLimit
///   - Options: Market, Limit
use lean_brokerages::BrokerageModel;
use lean_orders::security_transaction_model::{FlatFeeModel, SecurityTransactionModel};

/// Alpaca brokerage model.
///
/// `is_live` can be used to toggle live vs. paper trading behaviour in future
/// extensions; the brokerage model rules are identical for both modes.
pub struct AlpacaBrokerageModel {
    /// `true` when connected to live endpoints, `false` for paper trading.
    pub is_live: bool,
}

impl Default for AlpacaBrokerageModel {
    fn default() -> Self {
        Self { is_live: false }
    }
}

impl AlpacaBrokerageModel {
    pub fn new(is_live: bool) -> Self {
        Self { is_live }
    }
    pub fn paper() -> Self {
        Self::new(false)
    }
    pub fn live() -> Self {
        Self::new(true)
    }
}

impl BrokerageModel for AlpacaBrokerageModel {
    fn name(&self) -> &str {
        "Alpaca"
    }

    /// Alpaca charges $0 for equity trades.
    /// Crypto fees (0.15%–0.25%) are handled separately by the fee model
    /// and are not modelled here to keep the commission stub simple.
    fn transaction_model(&self) -> Box<dyn SecurityTransactionModel> {
        Box::new(FlatFeeModel::zero())
    }

    /// Default intraday leverage for equity margin accounts.
    ///
    /// Returns 4.0 (the intraday / pattern-day-trade multiplier).
    /// Overnight positions are limited to 2× by Alpaca's margin rules;
    /// callers that need overnight leverage should halve this value.
    fn default_leverage(&self) -> f64 {
        4.0
    }

    /// Alpaca accepts equities, crypto, and options orders.
    fn can_submit_order(&self) -> bool {
        true
    }

    /// Alpaca allows order updates (price, quantity, time-in-force).
    fn can_update_order(&self) -> bool {
        true
    }

    fn can_execute_order(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lean_orders::security_transaction_model::OrderFeeParameters;
    use lean_orders::OrderDirection;
    use rust_decimal_macros::dec;

    #[test]
    fn name() {
        assert_eq!(AlpacaBrokerageModel::default().name(), "Alpaca");
    }

    #[test]
    fn default_leverage_positive() {
        assert!(AlpacaBrokerageModel::default().default_leverage() > 0.0);
    }

    #[test]
    fn can_submit() {
        assert!(AlpacaBrokerageModel::default().can_submit_order());
    }

    #[test]
    fn fee_is_zero() {
        let fee = AlpacaBrokerageModel::default()
            .transaction_model()
            .get_order_fee(&OrderFeeParameters {
                security_price: dec!(100),
                order_quantity: dec!(10),
                order_direction: OrderDirection::Buy,
            });
        assert_eq!(fee.value, dec!(0));
        assert_eq!(fee.currency, "USD");
    }
}
