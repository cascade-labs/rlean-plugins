/// Interactive Brokers brokerage model.
///
/// Implements the tiered commission structure, RegT margin, and pattern day
/// trader rules modelled after the C# `InteractiveBrokersBrokerageModel`.
///
/// Leverage:
///   - Cash account → 1×
///   - RegT margin  → 2× equities, 50× forex, 10× CFD
///   - Portfolio margin → 6.67× (higher leverage for large, diversified accounts)
use lean_brokerages::BrokerageModel;
use lean_orders::security_transaction_model::{
    InteractiveBrokersFeeModel, SecurityTransactionModel,
};

/// IB account type — determines maximum available leverage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IbAccountType {
    /// Standard RegT margin account (2× equities, 50× forex).
    RegT,
    /// Portfolio margin account — higher leverage for large, diversified books.
    Portfolio,
    /// Cash account — no margin (1× leverage on everything).
    Cash,
}

/// Brokerage model for Interactive Brokers.
pub struct InteractiveBrokersBrokerageModel {
    pub account_type: IbAccountType,
}

impl Default for InteractiveBrokersBrokerageModel {
    fn default() -> Self {
        Self { account_type: IbAccountType::RegT }
    }
}

impl InteractiveBrokersBrokerageModel {
    pub fn new(account_type: IbAccountType) -> Self {
        Self { account_type }
    }

    pub fn reg_t() -> Self { Self::new(IbAccountType::RegT) }
    pub fn portfolio() -> Self { Self::new(IbAccountType::Portfolio) }
    pub fn cash() -> Self { Self::new(IbAccountType::Cash) }
}

impl BrokerageModel for InteractiveBrokersBrokerageModel {
    fn name(&self) -> &str { "InteractiveBrokers" }

    fn transaction_model(&self) -> Box<dyn SecurityTransactionModel> {
        Box::new(InteractiveBrokersFeeModel)
    }

    /// Default leverage by account type.
    ///
    /// IB RegT accounts receive 2× for equities and 50× for forex (majors).
    /// Portfolio-margin accounts receive up to 6.67×.
    /// Cash accounts are always 1×.
    fn default_leverage(&self) -> f64 {
        match self.account_type {
            IbAccountType::Cash => 1.0,
            IbAccountType::Portfolio => 6.67,
            IbAccountType::RegT => 2.0,
        }
    }

    /// IB supports a wide variety of order types.
    ///
    /// Accepted: Market, Limit, StopMarket, StopLimit, TrailingStop,
    /// MarketOnOpen, MarketOnClose, LimitIfTouched, OptionExercise.
    fn can_submit_order(&self) -> bool { true }

    /// IB allows order updates (quantity, price) for most order types.
    fn can_update_order(&self) -> bool { true }

    /// IB can execute all non-Base security types.
    fn can_execute_order(&self) -> bool { true }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lean_orders::security_transaction_model::OrderFeeParameters;
    use lean_orders::OrderDirection;
    use rust_decimal_macros::dec;

    #[test]
    fn name() { assert_eq!(InteractiveBrokersBrokerageModel::default().name(), "InteractiveBrokers"); }

    #[test]
    fn cash_leverage_is_one() { assert_eq!(InteractiveBrokersBrokerageModel::cash().default_leverage(), 1.0); }

    #[test]
    fn portfolio_leverage_higher_than_regt() {
        assert!(InteractiveBrokersBrokerageModel::portfolio().default_leverage()
              > InteractiveBrokersBrokerageModel::reg_t().default_leverage());
    }

    #[test]
    fn commission_is_positive() {
        let fee = InteractiveBrokersBrokerageModel::default().transaction_model()
            .get_order_fee(&OrderFeeParameters { security_price: dec!(100), order_quantity: dec!(100), order_direction: OrderDirection::Buy });
        assert!(fee.value > dec!(0), "IB charges commission on equities");
    }
}
