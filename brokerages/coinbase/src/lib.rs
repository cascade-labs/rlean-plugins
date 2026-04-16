/// Coinbase Advanced Trade (formerly GDAX / Coinbase Pro) brokerage model.
///
/// Coinbase uses a tiered maker/taker fee schedule based on 30-day trading
/// volume.  Margin trading is **not** supported — leverage is always 1×.
///
/// Volume tiers (approximate as of 2024):
///   0 — Basic  (< $10k/month):  0.60% maker / 0.80% taker
///   1 — Medium ($10k–$50k):     0.40% maker / 0.60% taker
///   2 — High   (> $50k):        0.20% maker / 0.30% taker  (and lower)
///
/// Supported order types: Market, Limit, StopLimit.
/// StopMarket was removed from Coinbase Pro on 2019-03-23.
///
/// 24/7 trading for all crypto assets.
use lean_brokerages::BrokerageModel;
use lean_orders::security_transaction_model::{
    BinanceFeeModel, SecurityTransactionModel,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

/// Coinbase Advanced Trade brokerage model.
///
/// `volume_tier` controls which fee band is used:
///   0 = Basic / lowest volume
///   1 = Medium volume
///   2 = High volume (institutional / VIP)
pub struct CoinbaseBrokerageModel {
    pub volume_tier: u32,
}

impl Default for CoinbaseBrokerageModel {
    fn default() -> Self { Self { volume_tier: 0 } }
}

impl CoinbaseBrokerageModel {
    pub fn new(volume_tier: u32) -> Self { Self { volume_tier } }

    /// Taker rate for the configured volume tier.
    pub fn taker_rate(&self) -> Decimal {
        match self.volume_tier {
            0 => dec!(0.008),  // 0.80%
            1 => dec!(0.006),  // 0.60%
            _ => dec!(0.003),  // 0.30% (high-volume tier and above)
        }
    }

    /// Maker rate for the configured volume tier.
    pub fn maker_rate(&self) -> Decimal {
        match self.volume_tier {
            0 => dec!(0.006),  // 0.60%
            1 => dec!(0.004),  // 0.40%
            _ => dec!(0.002),  // 0.20%
        }
    }
}

impl BrokerageModel for CoinbaseBrokerageModel {
    fn name(&self) -> &str { "Coinbase" }

    /// Returns a fee model approximating Coinbase's taker rate.
    ///
    /// We reuse `BinanceFeeModel` since both follow a taker/maker structure.
    fn transaction_model(&self) -> Box<dyn SecurityTransactionModel> {
        Box::new(BinanceFeeModel {
            taker_rate: self.taker_rate(),
            maker_rate: self.maker_rate(),
        })
    }

    /// Coinbase does not offer margin — leverage is always 1×.
    fn default_leverage(&self) -> f64 { 1.0 }

    /// Coinbase accepts crypto-only spot orders (Market, Limit, StopLimit).
    /// StopMarket orders are no longer accepted (removed 2019-03-23).
    fn can_submit_order(&self) -> bool { true }

    /// Coinbase only allows updates to GTC Limit orders.
    fn can_update_order(&self) -> bool { true }

    fn can_execute_order(&self) -> bool { true }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lean_orders::security_transaction_model::OrderFeeParameters;
    use lean_orders::OrderDirection;
    use rust_decimal_macros::dec;

    #[test]
    fn name() { assert_eq!(CoinbaseBrokerageModel::default().name(), "Coinbase"); }

    #[test]
    fn no_margin() { assert_eq!(CoinbaseBrokerageModel::default().default_leverage(), 1.0); }

    #[test]
    fn taker_rate_decreases_with_volume() {
        assert!(CoinbaseBrokerageModel::new(0).taker_rate() > CoinbaseBrokerageModel::new(2).taker_rate());
    }

    #[test]
    fn fee_positive() {
        let fee = CoinbaseBrokerageModel::default().transaction_model()
            .get_order_fee(&OrderFeeParameters { security_price: dec!(100), order_quantity: dec!(1), order_direction: OrderDirection::Buy });
        assert!(fee.value > dec!(0));
    }
}
