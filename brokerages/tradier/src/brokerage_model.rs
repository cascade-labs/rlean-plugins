/// Tradier brokerage model for backtesting.
///
/// Tradier offers commission-free equity trading, so the fee model returns $0.
/// Margin accounts get 2× leverage (consistent with Reg T / FINRA).
use lean_orders::security_transaction_model::{
    OrderFee, OrderFeeParameters, SecurityTransactionModel,
};

use lean_brokerages::BrokerageModel;

/// Tradier commission-free fee model.
///
/// Tradier charges $0 per equity trade (as of 2019).
pub struct TradierFeeModel;

impl SecurityTransactionModel for TradierFeeModel {
    fn get_order_fee(&self, _params: &OrderFeeParameters) -> OrderFee {
        OrderFee::zero()
    }
}

/// Brokerage model for Tradier — used during backtesting to pick the right
/// fee schedule and leverage.
pub struct TradierBrokerageModel;

impl BrokerageModel for TradierBrokerageModel {
    fn name(&self) -> &str {
        "Tradier"
    }

    fn transaction_model(&self) -> Box<dyn SecurityTransactionModel> {
        Box::new(TradierFeeModel)
    }

    /// 2× leverage matches Reg T margin (same as the C# default for US equity).
    fn default_leverage(&self) -> f64 {
        2.0
    }

    fn can_submit_order(&self) -> bool {
        true
    }
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
    use lean_orders::order::OrderDirection;
    use rust_decimal_macros::dec;

    fn params(price: rust_decimal::Decimal, qty: rust_decimal::Decimal) -> OrderFeeParameters {
        OrderFeeParameters {
            security_price: price,
            order_quantity: qty,
            order_direction: OrderDirection::Buy,
        }
    }

    #[test]
    fn equity_fee_is_zero() {
        let fee = TradierFeeModel.get_order_fee(&params(dec!(150), dec!(100)));
        assert_eq!(fee.value, dec!(0));
        assert_eq!(fee.currency, "USD");
    }

    #[test]
    fn large_order_fee_still_zero() {
        let fee = TradierFeeModel.get_order_fee(&params(dec!(500), dec!(1000)));
        assert_eq!(fee.value, dec!(0));
    }

    #[test]
    fn brokerage_model_name() {
        assert_eq!(TradierBrokerageModel.name(), "Tradier");
    }

    #[test]
    fn default_leverage_is_two() {
        assert_eq!(TradierBrokerageModel.default_leverage(), 2.0);
    }

    #[test]
    fn can_submit() {
        assert!(TradierBrokerageModel.can_submit_order());
    }

    #[test]
    fn transaction_model_zero_fee() {
        let fee = TradierBrokerageModel
            .transaction_model()
            .get_order_fee(&params(dec!(100), dec!(100)));
        assert_eq!(fee.value, dec!(0));
    }
}
