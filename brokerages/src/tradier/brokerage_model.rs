/// Tradier brokerage model for backtesting.
///
/// Tradier offers commission-free equity trading, so the fee model returns $0.
/// Margin accounts get 2× leverage (consistent with Reg T / FINRA).
use lean_orders::security_transaction_model::{OrderFee, OrderFeeParameters, SecurityTransactionModel};

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
    fn name(&self) -> &str { "Tradier" }

    fn transaction_model(&self) -> Box<dyn SecurityTransactionModel> {
        Box::new(TradierFeeModel)
    }

    /// 2× leverage matches Reg T margin (same as the C# default for US equity).
    fn default_leverage(&self) -> f64 { 2.0 }

    fn can_submit_order(&self) -> bool { true }
    fn can_update_order(&self) -> bool { true }
    fn can_execute_order(&self) -> bool { true }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lean_orders::OrderDirection;
    use rust_decimal_macros::dec;

    fn make_params(price: rust_decimal::Decimal, qty: rust_decimal::Decimal) -> OrderFeeParameters {
        OrderFeeParameters {
            security_price: price,
            order_quantity: qty,
            order_direction: OrderDirection::Buy,
        }
    }

    // ── TradierFeeModel (SecurityTransactionModel) ────────────────────────────

    #[test]
    fn test_tradier_equity_fee_is_zero() {
        // Tradier is commission-free for equities — every order must return $0.
        let model = TradierFeeModel;
        let fee = model.get_order_fee(&make_params(dec!(150.00), dec!(100)));
        assert_eq!(fee.value, dec!(0), "equity fee must be $0");
        assert_eq!(fee.currency, "USD");
    }

    #[test]
    fn test_tradier_equity_fee_large_order_still_zero() {
        // Even a 1,000-share buy should cost $0.
        let model = TradierFeeModel;
        let fee = model.get_order_fee(&make_params(dec!(500.00), dec!(1000)));
        assert_eq!(fee.value, dec!(0));
    }

    #[test]
    fn test_tradier_equity_fee_sell_order_zero() {
        // Sell orders are also commission-free.
        let model = TradierFeeModel;
        let params = OrderFeeParameters {
            security_price: dec!(200.00),
            order_quantity: dec!(-50),
            order_direction: OrderDirection::Sell,
        };
        let fee = model.get_order_fee(&params);
        assert_eq!(fee.value, dec!(0));
    }

    #[test]
    fn test_tradier_fee_currency_is_usd() {
        let model = TradierFeeModel;
        let fee = model.get_order_fee(&make_params(dec!(100), dec!(10)));
        assert_eq!(fee.currency, "USD");
    }

    // ── TradierBrokerageModel ─────────────────────────────────────────────────

    #[test]
    fn test_tradier_brokerage_model_name() {
        let model = TradierBrokerageModel;
        assert_eq!(model.name(), "Tradier");
    }

    #[test]
    fn test_tradier_default_leverage_is_two() {
        // Reg T / FINRA margin: 2× for equity accounts.
        let model = TradierBrokerageModel;
        assert_eq!(model.default_leverage(), 2.0);
    }

    #[test]
    fn test_tradier_can_submit_order() {
        let model = TradierBrokerageModel;
        assert!(model.can_submit_order());
    }

    #[test]
    fn test_tradier_can_update_order() {
        let model = TradierBrokerageModel;
        assert!(model.can_update_order());
    }

    #[test]
    fn test_tradier_can_execute_order() {
        let model = TradierBrokerageModel;
        assert!(model.can_execute_order());
    }

    #[test]
    fn test_tradier_transaction_model_returns_zero_fee() {
        // The model's transaction_model() should also produce zero fees.
        let model = TradierBrokerageModel;
        let txn = model.transaction_model();
        let fee = txn.get_order_fee(&make_params(dec!(100), dec!(200)));
        assert_eq!(fee.value, dec!(0));
    }
}
