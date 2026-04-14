/// FXCM brokerage model.
///
/// FXCM is a commission-based forex and CFD broker.  Unlike OANDA (spread
/// only), FXCM charges a per-lot commission in addition to the spread:
///   - Standard lots: ~$4 per lot (round-turn ~$8)
///   - Mini lots:     ~$0.40 per lot (round-turn ~$0.80) on some accounts
///
/// Leverage limits (US retail, pre-ESMA):
///   - Major forex pairs: up to 400:1
///   - Minor / exotic forex: up to 200:1
///   - CFDs: varies by instrument
///
/// Only Forex and CFD securities are supported.
///
/// Trading hours: 24 hours / 5 days.
///
/// Supported order types: Market, Limit, StopMarket.
/// Orders must satisfy:
///   - Quantity is a whole multiple of the instrument's lot size.
///   - Limit/stop prices must be within 15,000 pips *and* 50% of current
///     price (whichever is tighter).
///   - Time-in-force must be GoodTilCanceled.
use lean_brokerages::BrokerageModel;
use lean_orders::security_transaction_model::{
    FlatFeeModel, SecurityTransactionModel,
};

/// Brokerage model for FXCM.
pub struct FxcmBrokerageModel;

impl BrokerageModel for FxcmBrokerageModel {
    fn name(&self) -> &str { "FXCM" }

    /// FXCM charges approximately $4 per standard lot per side.
    ///
    /// The exact per-trade amount depends on the account tier and instrument.
    /// We approximate this with a flat $4 fee per order submission.  For
    /// round-trip cost, callers should double this (buy + sell).
    fn transaction_model(&self) -> Box<dyn SecurityTransactionModel> {
        use rust_decimal_macros::dec;
        Box::new(FlatFeeModel::new(dec!(4.0)))
    }

    /// FXCM offers up to 400:1 leverage on major forex pairs for US retail
    /// accounts.  Minor pairs and CFDs receive lower leverage.
    /// We return 400.0 as the account-wide upper bound.
    fn default_leverage(&self) -> f64 { 400.0 }

    /// FXCM accepts Forex and CFD orders.
    /// Supported types: Market, Limit, StopMarket (GTC only).
    /// Quantities must be whole multiples of the instrument's lot size.
    fn can_submit_order(&self) -> bool { true }

    /// FXCM allows order modifications subject to the same lot-size and
    /// price-distance constraints as new order submission.
    fn can_update_order(&self) -> bool { true }

    fn can_execute_order(&self) -> bool { true }
}
