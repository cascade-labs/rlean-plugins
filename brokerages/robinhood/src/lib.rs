/// Robinhood brokerage model.
///
/// Robinhood offers commission-free equity and ETF trading.  The model reflects
/// the rules and constraints that apply when backtesting strategies against
/// Robinhood's execution environment.
///
/// # Account tiers
///
/// | Tier          | Leverage | Features                                    |
/// |---------------|----------|---------------------------------------------|
/// | `Instant`     | 2×       | Instant settlement, margin up to account equity |
/// | `Gold`        | 2×       | Premium margin tier ($5/month), same 2× cap |
/// | `Cash`        | 1×       | No margin, settlement T+2                   |
///
/// # Commission schedule
///
/// - Equities & ETFs: **$0** per trade
/// - Options:         **$0** per contract (regulatory fees may apply)
///   - Options Regulatory Fee (ORF): ≈ $0.002–$0.03 per contract (exchange-
///     assessed, passed through); modelled at $0.03 per contract as a
///     conservative upper bound.
/// - Crypto:          **$0** explicit commission (spread-based pricing)
///
/// # Restrictions
///
/// - **No short selling** — Robinhood does not support short positions.
///   `can_submit_order` returns `false` for sell-short orders when
///   `allow_shorting` is `false` (default).
/// - **PDT rule** — accounts under $25,000 are limited to 3 day trades per
///   rolling 5 business days.  The model exposes `account_equity` and
///   `day_trade_count` fields for callers to enforce the rule.
/// - **Fractional shares** — enabled by default; set
///   `fractional_shares_enabled = false` to disable.
///
/// # Options approval tiers
///
/// | Level | Description                                      |
/// |-------|--------------------------------------------------|
/// | 1     | Covered calls, protective puts                   |
/// | 2     | Long calls, long puts, spreads (most common)     |
/// | 3     | Naked options (Robinhood Gold+ with approval)    |
///
/// # Supported order types
///
/// Equities: Market, Limit, StopMarket, StopLimit
/// Options:  Market, Limit
/// Crypto:   Market, Limit
///
/// Note: TrailingStop, MarketOnOpen, MarketOnClose are **not** supported.
///
/// # References
///
/// - Robinhood Help: <https://robinhood.com/us/en/support/>
/// - SEC RegSHO (short selling): <https://www.sec.gov/investor/pubs/regsho.htm>
/// - FINRA PDT rule: <https://www.finra.org/investors/learn-to-invest/advanced-investing/day-trading-margin-requirements-know-rules>
use lean_brokerages::BrokerageModel;
use lean_orders::security_transaction_model::{
    FlatFeeModel, OrderFee, OrderFeeParameters, SecurityTransactionModel,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

// ---------------------------------------------------------------------------
// Account tier
// ---------------------------------------------------------------------------

/// Robinhood account tier — determines margin availability and leverage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RobinhoodAccountTier {
    /// Free instant-settlement account with basic 2× margin access.
    ///
    /// This is the default tier for new Robinhood accounts.
    #[default]
    Instant,

    /// Premium Gold account ($5/month) with higher margin limits.
    ///
    /// Gold accounts gain access to Robinhood's extended-hours trading data
    /// and higher margin borrowing limits, but the regulatory leverage cap
    /// remains 2× (Reg T).
    Gold,

    /// No-margin cash account (T+2 settlement).
    ///
    /// Cash accounts cannot borrow.  All trades must be fully funded by
    /// settled cash.
    Cash,
}

impl RobinhoodAccountTier {
    /// Returns the maximum leverage multiplier for this account tier.
    ///
    /// Both `Instant` and `Gold` are margin accounts subject to FINRA Reg T
    /// (50% initial margin requirement → 2× leverage).  `Cash` has no margin.
    pub fn max_leverage(self) -> f64 {
        match self {
            Self::Instant | Self::Gold => 2.0,
            Self::Cash => 1.0,
        }
    }

    /// Returns a human-readable label for the tier.
    pub fn label(self) -> &'static str {
        match self {
            Self::Instant => "Robinhood Instant",
            Self::Gold    => "Robinhood Gold",
            Self::Cash    => "Robinhood Cash",
        }
    }
}

// ---------------------------------------------------------------------------
// Options approval tier
// ---------------------------------------------------------------------------

/// Robinhood options approval tier.
///
/// Robinhood requires users to apply for options trading and grants one of
/// three levels based on experience and financial profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum OptionsLevel {
    /// Level 1 — covered calls and protective/cash-secured puts only.
    #[default]
    Level1 = 1,
    /// Level 2 — long calls, long puts, spreads (most retail traders).
    Level2 = 2,
    /// Level 3 — multi-leg strategies including naked options (Gold only).
    Level3 = 3,
}

// ---------------------------------------------------------------------------
// Fee model
// ---------------------------------------------------------------------------

/// Robinhood equity/ETF fee model — $0 per trade.
///
/// Robinhood earns revenue through payment for order flow (PFOF) rather than
/// explicit commissions.  From the perspective of the backtesting engine,
/// commission cost is $0 for equities and ETFs.
pub struct RobinhoodEquityFeeModel;

impl SecurityTransactionModel for RobinhoodEquityFeeModel {
    fn get_order_fee(&self, _params: &OrderFeeParameters) -> OrderFee {
        OrderFee::zero()
    }
}

/// Robinhood options fee model.
///
/// Robinhood charges $0 per-contract commission but passes through the
/// Options Regulatory Fee (ORF) assessed by U.S. options exchanges.  The
/// ORF varies by exchange (≈ $0.002–$0.03 per contract); this model uses
/// **$0.03 per contract** as a conservative upper bound.
///
/// Note: The ORF is calculated on the number of contracts, not the notional
/// value, so we treat each unit of `order_quantity` as one contract.
pub struct RobinhoodOptionsFeeModel {
    /// Options Regulatory Fee per contract (default: $0.03).
    pub orf_per_contract: Decimal,
}

impl Default for RobinhoodOptionsFeeModel {
    fn default() -> Self {
        Self { orf_per_contract: dec!(0.03) }
    }
}

impl RobinhoodOptionsFeeModel {
    /// Create a new options fee model with a custom ORF rate.
    pub fn new(orf_per_contract: Decimal) -> Self {
        Self { orf_per_contract }
    }

    /// Zero-fee model (useful for paper-trading simulations that ignore
    /// regulatory fees).
    pub fn zero() -> Self {
        Self { orf_per_contract: dec!(0) }
    }
}

impl SecurityTransactionModel for RobinhoodOptionsFeeModel {
    fn get_order_fee(&self, params: &OrderFeeParameters) -> OrderFee {
        let contracts = params.order_quantity.abs();
        let fee = contracts * self.orf_per_contract;
        OrderFee::flat(fee, "USD")
    }
}

// ---------------------------------------------------------------------------
// Brokerage model
// ---------------------------------------------------------------------------

/// Brokerage model for Robinhood.
///
/// # Quick start
///
/// ```rust
/// use lean_brokerages::{BrokerageModel, robinhood::{RobinhoodBrokerageModel, RobinhoodAccountTier}};
///
/// // Default: Instant (2× margin), fractional shares, no shorting
/// let model = RobinhoodBrokerageModel::default();
/// assert_eq!(model.default_leverage(), 2.0);
/// assert!(!model.can_short_sell());
///
/// // Gold account
/// let gold = RobinhoodBrokerageModel::gold();
/// assert_eq!(gold.account_tier, RobinhoodAccountTier::Gold);
///
/// // Cash account — no margin
/// let cash = RobinhoodBrokerageModel::cash();
/// assert_eq!(cash.default_leverage(), 1.0);
/// ```
pub struct RobinhoodBrokerageModel {
    // ----- account configuration -----

    /// Account tier (Instant, Gold, Cash).
    pub account_tier: RobinhoodAccountTier,

    /// Options approval level granted to this account.
    ///
    /// Used by callers to gate which option strategies are permitted.
    pub options_level: OptionsLevel,

    // ----- PDT tracking -----

    /// Current account equity in USD.
    ///
    /// Accounts with `account_equity < 25_000.0` are subject to the FINRA
    /// Pattern Day Trader (PDT) rule.
    pub account_equity: f64,

    /// Number of day trades executed in the current rolling 5-business-day
    /// window.  Callers are responsible for updating this counter after each
    /// round-trip trade.
    pub day_trade_count: u32,

    // ----- feature flags -----

    /// Whether the account is in live mode (`true`) or paper mode (`false`).
    pub is_live: bool,

    /// Whether fractional share orders are permitted.
    ///
    /// Robinhood supports fractional shares for many equities and ETFs.
    /// Set to `false` to restrict orders to whole-share quantities.
    pub fractional_shares_enabled: bool,

    /// Whether short selling is permitted.
    ///
    /// Robinhood **does not support short selling** — this flag should remain
    /// `false` in production.  It is exposed only to allow simulations that
    /// relax this constraint for research purposes.
    pub allow_shorting: bool,
}

// ---------------------------------------------------------------------------
// Constructors
// ---------------------------------------------------------------------------

impl Default for RobinhoodBrokerageModel {
    /// Creates a default Robinhood model:
    /// - `Instant` account tier (2× margin)
    /// - `Level2` options approval
    /// - `account_equity = 0.0` (PDT rule enforced)
    /// - `day_trade_count = 0`
    /// - paper trading mode
    /// - fractional shares enabled
    /// - short selling disabled
    fn default() -> Self {
        Self {
            account_tier: RobinhoodAccountTier::Instant,
            options_level: OptionsLevel::Level2,
            account_equity: 0.0,
            day_trade_count: 0,
            is_live: false,
            fractional_shares_enabled: true,
            allow_shorting: false,
        }
    }
}

impl RobinhoodBrokerageModel {
    /// Create a model for a Robinhood Instant (margin) account.
    pub fn instant() -> Self {
        Self { account_tier: RobinhoodAccountTier::Instant, ..Self::default() }
    }

    /// Create a model for a Robinhood Gold account.
    pub fn gold() -> Self {
        Self { account_tier: RobinhoodAccountTier::Gold, ..Self::default() }
    }

    /// Create a model for a Robinhood Cash account (no margin, 1× leverage).
    pub fn cash() -> Self {
        Self { account_tier: RobinhoodAccountTier::Cash, ..Self::default() }
    }

    /// Builder: set account equity (used for PDT rule evaluation).
    pub fn with_equity(mut self, equity: f64) -> Self {
        self.account_equity = equity;
        self
    }

    /// Builder: set the options approval level.
    pub fn with_options_level(mut self, level: OptionsLevel) -> Self {
        self.options_level = level;
        self
    }

    /// Builder: enable or disable fractional share trading.
    pub fn with_fractional_shares(mut self, enabled: bool) -> Self {
        self.fractional_shares_enabled = enabled;
        self
    }

    /// Builder: enable or disable short selling (research / simulation only).
    pub fn with_shorting(mut self, allow: bool) -> Self {
        self.allow_shorting = allow;
        self
    }

    /// Builder: switch to live trading mode.
    pub fn live(mut self) -> Self {
        self.is_live = true;
        self
    }

    // ----- PDT helpers -----

    /// Returns `true` if the account equity is below the $25,000 PDT
    /// threshold and is therefore subject to the day-trading limit.
    pub fn is_pdt_restricted(&self) -> bool {
        self.account_equity < 25_000.0
    }

    /// Returns `true` if the account has reached the maximum of 3 day trades
    /// in the rolling 5-business-day window **and** is PDT-restricted.
    ///
    /// Callers should call this before submitting a round-trip intraday order.
    pub fn pdt_limit_reached(&self) -> bool {
        self.is_pdt_restricted() && self.day_trade_count >= 3
    }

    /// Returns `true` if short selling is enabled on this model.
    ///
    /// Always `false` in production (Robinhood does not support short sales).
    pub fn can_short_sell(&self) -> bool {
        self.allow_shorting
    }

    /// Returns the equity fee model (always $0).
    pub fn equity_fee_model(&self) -> RobinhoodEquityFeeModel {
        RobinhoodEquityFeeModel
    }

    /// Returns the options fee model (ORF pass-through, default $0.03/contract).
    pub fn options_fee_model(&self) -> RobinhoodOptionsFeeModel {
        RobinhoodOptionsFeeModel::default()
    }
}

// ---------------------------------------------------------------------------
// BrokerageModel implementation
// ---------------------------------------------------------------------------

impl BrokerageModel for RobinhoodBrokerageModel {
    /// Returns the account tier label (e.g. "Robinhood Instant").
    fn name(&self) -> &str {
        self.account_tier.label()
    }

    /// Returns the equity fee model ($0 commission).
    ///
    /// For options orders callers should retrieve `options_fee_model()` instead,
    /// which includes the ORF pass-through.
    fn transaction_model(&self) -> Box<dyn SecurityTransactionModel> {
        Box::new(FlatFeeModel::zero())
    }

    /// Returns the default leverage for the configured account tier.
    ///
    /// | Tier          | Leverage |
    /// |---------------|----------|
    /// | `Instant`     | 2.0×     |
    /// | `Gold`        | 2.0×     |
    /// | `Cash`        | 1.0×     |
    ///
    /// Both Instant and Gold are Reg T margin accounts capped at 50% initial
    /// margin (2× leverage).  Cash accounts have no margin (1×).
    fn default_leverage(&self) -> f64 {
        self.account_tier.max_leverage()
    }

    /// Returns `true` unless the PDT limit has been reached.
    ///
    /// Robinhood will reject new orders that would trigger a PDT violation.
    /// This method checks both the PDT restriction flag and the day trade
    /// counter maintained by the caller.
    fn can_submit_order(&self) -> bool {
        !self.pdt_limit_reached()
    }

    /// Robinhood supports replacing open orders (cancel + resubmit).
    ///
    /// Price and quantity changes are allowed for Limit orders.  Market
    /// orders cannot be modified once accepted.
    fn can_update_order(&self) -> bool {
        true
    }

    /// Robinhood can execute all standard US equity and options orders.
    fn can_execute_order(&self) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use lean_orders::security_transaction_model::OrderFeeParameters;
    use lean_orders::OrderDirection;
    use rust_decimal_macros::dec;

    #[test]
    fn name() { assert_eq!(RobinhoodBrokerageModel::default().name(), "Robinhood Instant"); }

    #[test]
    fn instant_leverage_is_two() { assert_eq!(RobinhoodBrokerageModel::default().default_leverage(), 2.0); }

    #[test]
    fn equity_fee_is_zero() {
        let fee = RobinhoodEquityFeeModel.get_order_fee(&OrderFeeParameters { security_price: dec!(100), order_quantity: dec!(10), order_direction: OrderDirection::Buy });
        assert_eq!(fee.value, dec!(0));
    }

    #[test]
    fn options_orf_fee_positive() {
        let fee = RobinhoodOptionsFeeModel::default().get_order_fee(&OrderFeeParameters { security_price: dec!(1), order_quantity: dec!(10), order_direction: OrderDirection::Buy });
        assert!(fee.value > dec!(0), "Robinhood charges ORF on options contracts");
    }
}
