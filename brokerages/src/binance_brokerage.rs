/// Binance brokerage model.
///
/// Covers three distinct Binance markets:
///   - Spot         — 0.10% taker/maker, up to 3× margin (cross), 1× cash
///   - USDT Futures — 0.02% maker / 0.055% taker, up to 125× leverage
///   - Coin Futures — 0.02% maker / 0.055% taker, up to 125× leverage
///
/// Binance does **not** support updating existing orders — they must be
/// cancelled and resubmitted.
///
/// Supported order types (Spot & Futures):
///   Market, Limit, StopLimit, StopMarket (futures only), TrailingStop
///
/// 24/7 trading on all markets.
use lean_brokerages::BrokerageModel;
use lean_orders::security_transaction_model::{
    BinanceFeeModel, SecurityTransactionModel,
};
use rust_decimal_macros::dec;

/// Which Binance product line this model represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinanceMarket {
    /// Standard spot market (BTC/USDT etc.).
    Spot,
    /// USDT-margined perpetual futures (BTCUSDT-PERP etc.).
    UsdtFutures,
    /// Coin-margined perpetual futures (BTCUSD-PERP etc.).
    CoinFutures,
}

/// Brokerage model for Binance.
pub struct BinanceBrokerageModel {
    pub market: BinanceMarket,
}

impl Default for BinanceBrokerageModel {
    fn default() -> Self { Self { market: BinanceMarket::Spot } }
}

impl BinanceBrokerageModel {
    pub fn new(market: BinanceMarket) -> Self { Self { market } }
    pub fn spot() -> Self { Self::new(BinanceMarket::Spot) }
    pub fn usdt_futures() -> Self { Self::new(BinanceMarket::UsdtFutures) }
    pub fn coin_futures() -> Self { Self::new(BinanceMarket::CoinFutures) }
}

impl BrokerageModel for BinanceBrokerageModel {
    fn name(&self) -> &str { "Binance" }

    fn transaction_model(&self) -> Box<dyn SecurityTransactionModel> {
        match self.market {
            BinanceMarket::Spot => Box::new(BinanceFeeModel {
                taker_rate: dec!(0.001),  // 0.10%
                maker_rate: dec!(0.001),  // 0.10%
            }),
            BinanceMarket::UsdtFutures | BinanceMarket::CoinFutures => {
                Box::new(BinanceFeeModel {
                    taker_rate: dec!(0.00055), // 0.055%
                    maker_rate: dec!(0.0002),  // 0.02%
                })
            }
        }
    }

    /// Default leverage by market.
    ///
    /// Spot: 3× cross-margin (or 1× for cash accounts — callers should
    ///       check account settings separately).
    /// Futures: 25× default (max 125× but that is not the model default).
    fn default_leverage(&self) -> f64 {
        match self.market {
            BinanceMarket::Spot => 3.0,
            BinanceMarket::UsdtFutures | BinanceMarket::CoinFutures => 25.0,
        }
    }

    /// Binance accepts all standard crypto order types.
    fn can_submit_order(&self) -> bool { true }

    /// Binance does **not** support in-place order updates.
    /// Cancel the old order and submit a new one.
    fn can_update_order(&self) -> bool { false }

    fn can_execute_order(&self) -> bool { true }
}
