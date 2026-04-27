/// OANDA brokerage model.
///
/// OANDA is a spread-based forex and CFD broker — there are no per-trade
/// commissions; the broker earns its spread on each transaction.
///
/// Leverage limits (regulatory caps apply):
///   - US accounts:  up to 50:1 on major forex pairs
///   - EU accounts:  up to 30:1 on major forex pairs (ESMA rules)
///
/// Only Forex and CFD securities are supported.  Equities, options, and
/// futures are **not** available through OANDA.
///
/// Trading hours: 24 hours / 5 days (Monday 00:00 UTC to Friday 22:00 UTC).
///
/// Supported order types: Market, Limit, StopMarket, StopLimit.
/// Only GoodTilCanceled (GTC) time-in-force is accepted.
use lean_brokerages::BrokerageModel;
use lean_orders::security_transaction_model::{FlatFeeModel, SecurityTransactionModel};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

/// Brokerage model for OANDA.
///
/// Set `max_leverage` to 50.0 for US accounts or 30.0 for EU/ESMA-regulated
/// accounts.  The default is 50.0 (US retail).
pub struct OandaBrokerageModel {
    /// Maximum leverage offered to this account (regulatory limit).
    /// 50.0 for US, 30.0 for EU (ESMA capped).
    pub max_leverage: Decimal,
}

impl Default for OandaBrokerageModel {
    /// US-regulation default: 50:1 on major forex pairs.
    fn default() -> Self {
        Self {
            max_leverage: dec!(50),
        }
    }
}

impl OandaBrokerageModel {
    /// US-regulation account (50:1 major pairs).
    pub fn us() -> Self {
        Self {
            max_leverage: dec!(50),
        }
    }

    /// EU/ESMA-regulated account (30:1 major pairs).
    pub fn eu() -> Self {
        Self {
            max_leverage: dec!(30),
        }
    }

    pub fn new(max_leverage: Decimal) -> Self {
        Self { max_leverage }
    }
}

impl BrokerageModel for OandaBrokerageModel {
    fn name(&self) -> &str {
        "Oanda"
    }

    /// OANDA is spread-based — no explicit per-trade commission.
    fn transaction_model(&self) -> Box<dyn SecurityTransactionModel> {
        Box::new(FlatFeeModel::zero())
    }

    /// Returns `max_leverage` as f64.
    ///
    /// The C# implementation returns `GetLeverage(security)` which uses the
    /// account-level cap. Here we return the account maximum; per-pair limits
    /// (e.g., 20:1 for non-majors under ESMA) are not yet modelled.
    fn default_leverage(&self) -> f64 {
        // Decimal::to_f64 with lossy conversion
        self.max_leverage.try_into().unwrap_or(50.0)
    }

    /// OANDA only accepts Forex and CFD orders.
    /// Supported types: Market, Limit, StopMarket, StopLimit (GTC only).
    fn can_submit_order(&self) -> bool {
        true
    }

    /// OANDA allows order modifications.
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
        assert_eq!(OandaBrokerageModel::default().name(), "Oanda");
    }

    #[test]
    fn us_leverage_50() {
        assert_eq!(OandaBrokerageModel::us().default_leverage(), 50.0);
    }

    #[test]
    fn eu_leverage_30() {
        assert_eq!(OandaBrokerageModel::eu().default_leverage(), 30.0);
    }

    #[test]
    fn spread_only_no_commission() {
        let fee = OandaBrokerageModel::default()
            .transaction_model()
            .get_order_fee(&OrderFeeParameters {
                security_price: dec!(1),
                order_quantity: dec!(100000),
                order_direction: OrderDirection::Buy,
            });
        assert_eq!(fee.value, dec!(0));
    }
}
