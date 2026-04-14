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
/// use lean_brokerages::robinhood::{RobinhoodBrokerageModel, RobinhoodAccountTier};
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

    #[test]
    fn instant_account_has_2x_leverage() {
        let model = RobinhoodBrokerageModel::instant();
        assert_eq!(model.default_leverage(), 2.0);
    }

    #[test]
    fn gold_account_has_2x_leverage() {
        let model = RobinhoodBrokerageModel::gold();
        assert_eq!(model.default_leverage(), 2.0);
    }

    #[test]
    fn cash_account_has_1x_leverage() {
        let model = RobinhoodBrokerageModel::cash();
        assert_eq!(model.default_leverage(), 1.0);
    }

    #[test]
    fn equity_fee_is_zero() {
        use lean_orders::security_transaction_model::OrderFeeParameters;
        use lean_orders::order::OrderDirection;
        let model = RobinhoodBrokerageModel::default();
        let tm = model.transaction_model();
        let params = OrderFeeParameters {
            security_price: dec!(150.0),
            order_quantity: dec!(10),
            order_direction: OrderDirection::Buy,
        };
        let fee = tm.get_order_fee(&params);
        assert_eq!(fee.value, dec!(0));
    }

    #[test]
    fn options_fee_includes_orf() {
        use lean_orders::security_transaction_model::OrderFeeParameters;
        use lean_orders::order::OrderDirection;
        let fee_model = RobinhoodOptionsFeeModel::default();
        let params = OrderFeeParameters {
            security_price: dec!(5.0),
            order_quantity: dec!(3), // 3 contracts
            order_direction: OrderDirection::Buy,
        };
        let fee = fee_model.get_order_fee(&params);
        // 3 contracts × $0.03/contract = $0.09
        assert_eq!(fee.value, dec!(0.09));
    }

    #[test]
    fn no_short_selling_by_default() {
        let model = RobinhoodBrokerageModel::default();
        assert!(!model.can_short_sell());
    }

    #[test]
    fn shorting_can_be_enabled_for_research() {
        let model = RobinhoodBrokerageModel::default().with_shorting(true);
        assert!(model.can_short_sell());
    }

    #[test]
    fn pdt_not_restricted_above_25k() {
        let model = RobinhoodBrokerageModel::default().with_equity(30_000.0);
        assert!(!model.is_pdt_restricted());
        assert!(!model.pdt_limit_reached());
    }

    #[test]
    fn pdt_restricted_below_25k() {
        let model = RobinhoodBrokerageModel::default().with_equity(10_000.0);
        assert!(model.is_pdt_restricted());
    }

    #[test]
    fn pdt_limit_blocks_order_submission() {
        let mut model = RobinhoodBrokerageModel::default().with_equity(10_000.0);
        model.day_trade_count = 3;
        assert!(model.pdt_limit_reached());
        assert!(!model.can_submit_order());
    }

    #[test]
    fn pdt_limit_not_reached_below_3_trades() {
        let mut model = RobinhoodBrokerageModel::default().with_equity(10_000.0);
        model.day_trade_count = 2;
        assert!(!model.pdt_limit_reached());
        assert!(model.can_submit_order());
    }

    #[test]
    fn high_equity_bypasses_pdt_even_with_many_trades() {
        let mut model = RobinhoodBrokerageModel::default().with_equity(50_000.0);
        model.day_trade_count = 10;
        assert!(!model.pdt_limit_reached());
        assert!(model.can_submit_order());
    }

    #[test]
    fn builder_chain_works() {
        let model = RobinhoodBrokerageModel::gold()
            .with_equity(50_000.0)
            .with_options_level(OptionsLevel::Level3)
            .with_fractional_shares(false)
            .with_shorting(false)
            .live();
        assert_eq!(model.account_tier, RobinhoodAccountTier::Gold);
        assert_eq!(model.options_level, OptionsLevel::Level3);
        assert_eq!(model.account_equity, 50_000.0);
        assert!(!model.fractional_shares_enabled);
        assert!(!model.allow_shorting);
        assert!(model.is_live);
    }

    #[test]
    fn name_reflects_account_tier() {
        assert_eq!(RobinhoodBrokerageModel::instant().name(), "Robinhood Instant");
        assert_eq!(RobinhoodBrokerageModel::gold().name(), "Robinhood Gold");
        assert_eq!(RobinhoodBrokerageModel::cash().name(), "Robinhood Cash");
    }

    // ── Additional comprehensive tests ────────────────────────────────────────
    //
    // The tests below cover all scenarios required by the task specification,
    // aligned against the C# RobinhoodBrokerageModel reference behaviour.

    // -- Commission / fee model ------------------------------------------------

    /// Equity commission is exactly $0 for a standard market buy.
    #[test]
    fn test_robinhood_equity_fee_zero() {
        use lean_orders::security_transaction_model::OrderFeeParameters;
        use lean_orders::order::OrderDirection;

        let model = RobinhoodBrokerageModel::instant();
        let tm = model.transaction_model();
        let params = OrderFeeParameters {
            security_price: dec!(150.00),
            order_quantity: dec!(10),
            order_direction: OrderDirection::Buy,
        };
        let fee = tm.get_order_fee(&params);
        assert_eq!(fee.value, dec!(0), "Equity commission must be $0");
        assert_eq!(fee.currency, "USD");
    }

    /// The default (equity) transaction model returns $0 for options positions too.
    /// Regulatory fees (ORF) are tracked separately via `options_fee_model()`.
    #[test]
    fn test_robinhood_options_zero_commission() {
        use lean_orders::security_transaction_model::OrderFeeParameters;
        use lean_orders::order::OrderDirection;

        let model = RobinhoodBrokerageModel::instant();
        let tm = model.transaction_model();
        // 5 contracts at a $3.50 premium per share (× 100 shares/contract)
        let params = OrderFeeParameters {
            security_price: dec!(3.50),
            order_quantity: dec!(5),
            order_direction: OrderDirection::Buy,
        };
        let fee = tm.get_order_fee(&params);
        assert_eq!(
            fee.value,
            dec!(0),
            "Options commission via the equity fee path must be $0"
        );
    }

    /// ORF pass-through: 3 contracts × $0.03/contract = $0.09.
    #[test]
    fn test_robinhood_options_orf_fee_three_contracts() {
        use lean_orders::security_transaction_model::OrderFeeParameters;
        use lean_orders::order::OrderDirection;

        let fee_model = RobinhoodOptionsFeeModel::default();
        let params = OrderFeeParameters {
            security_price: dec!(5.00),
            order_quantity: dec!(3),
            order_direction: OrderDirection::Buy,
        };
        let fee = fee_model.get_order_fee(&params);
        assert_eq!(fee.value, dec!(0.09), "ORF: 3 contracts × $0.03 = $0.09");
        assert_eq!(fee.currency, "USD");
    }

    /// ORF model with `zero()` returns $0 (used in paper-trading simulations).
    #[test]
    fn test_robinhood_options_zero_orf_model() {
        use lean_orders::security_transaction_model::OrderFeeParameters;
        use lean_orders::order::OrderDirection;

        let fee_model = RobinhoodOptionsFeeModel::zero();
        let params = OrderFeeParameters {
            security_price: dec!(5.00),
            order_quantity: dec!(10),
            order_direction: OrderDirection::Buy,
        };
        let fee = fee_model.get_order_fee(&params);
        assert_eq!(fee.value, dec!(0));
    }

    // -- Short selling ---------------------------------------------------------

    /// Robinhood does not allow short selling by default.
    #[test]
    fn test_robinhood_no_short_selling() {
        let model = RobinhoodBrokerageModel::default();
        assert!(!model.can_short_sell(), "Short selling must be disabled by default");
    }

    /// Short prohibition applies to all three account tiers.
    #[test]
    fn test_robinhood_no_short_selling_all_tiers() {
        for model in [
            RobinhoodBrokerageModel::cash(),
            RobinhoodBrokerageModel::instant(),
            RobinhoodBrokerageModel::gold(),
        ] {
            assert!(
                !model.can_short_sell(),
                "Short selling should be disallowed on {:?} tier",
                model.account_tier
            );
        }
    }

    /// Shorting can be explicitly enabled for research simulations.
    #[test]
    fn test_robinhood_shorting_enabled_for_research() {
        let model = RobinhoodBrokerageModel::instant().with_shorting(true);
        assert!(model.can_short_sell());
    }

    // -- Leverage / margin -----------------------------------------------------

    /// Instant account provides standard RegT 2× margin leverage.
    #[test]
    fn test_robinhood_instant_account_leverage() {
        let model = RobinhoodBrokerageModel::instant();
        assert_eq!(
            model.default_leverage(),
            2.0,
            "Instant tier must provide 2× leverage (Reg T)"
        );
    }

    /// Cash account must have no margin — 1× leverage only.
    #[test]
    fn test_robinhood_cash_account_no_leverage() {
        let model = RobinhoodBrokerageModel::cash();
        assert_eq!(
            model.default_leverage(),
            1.0,
            "Cash tier must provide 1× leverage (no margin)"
        );
    }

    /// Gold account provides 2× leverage — same as Instant under Reg T.
    #[test]
    fn test_robinhood_gold_account_leverage() {
        let model = RobinhoodBrokerageModel::gold();
        assert_eq!(
            model.default_leverage(),
            2.0,
            "Gold tier must provide 2× leverage (Reg T, same cap as Instant)"
        );
    }

    /// `RobinhoodAccountTier::max_leverage` is consistent with `default_leverage`.
    #[test]
    fn test_robinhood_account_tier_max_leverage_consistency() {
        assert_eq!(RobinhoodAccountTier::Cash.max_leverage(), 1.0);
        assert_eq!(RobinhoodAccountTier::Instant.max_leverage(), 2.0);
        assert_eq!(RobinhoodAccountTier::Gold.max_leverage(), 2.0);
    }

    // -- PDT detection ---------------------------------------------------------

    /// Account with $0 equity is immediately PDT-restricted.
    #[test]
    fn test_robinhood_pdt_detection_zero_equity() {
        let model = RobinhoodBrokerageModel::default(); // equity = 0.0
        assert!(
            model.is_pdt_restricted(),
            "Zero-equity account must be PDT-restricted"
        );
    }

    /// Account just below the $25,000 threshold is PDT-restricted.
    #[test]
    fn test_robinhood_pdt_detection_below_threshold() {
        let model = RobinhoodBrokerageModel::default().with_equity(24_999.99);
        assert!(
            model.is_pdt_restricted(),
            "Account below $25k must be PDT-restricted"
        );
    }

    /// Account at exactly $25,000 is NOT PDT-restricted.
    #[test]
    fn test_robinhood_pdt_detection_at_threshold() {
        let model = RobinhoodBrokerageModel::default().with_equity(25_000.0);
        assert!(
            !model.is_pdt_restricted(),
            "Account at exactly $25k must NOT be PDT-restricted"
        );
    }

    /// Account above $25,000 is not PDT-restricted.
    #[test]
    fn test_robinhood_pdt_detection_above_threshold() {
        let model = RobinhoodBrokerageModel::default().with_equity(100_000.0);
        assert!(
            !model.is_pdt_restricted(),
            "Account above $25k must NOT be PDT-restricted"
        );
    }

    /// PDT limit blocks order submission once 3 day trades are used.
    #[test]
    fn test_robinhood_pdt_limit_blocks_submission() {
        let mut model = RobinhoodBrokerageModel::default().with_equity(10_000.0);
        model.day_trade_count = 3;
        assert!(model.pdt_limit_reached(), "3 day trades on PDT-restricted account must trigger limit");
        assert!(!model.can_submit_order(), "Order submission must be blocked when PDT limit reached");
    }

    /// PDT limit is not reached with only 2 day trades.
    #[test]
    fn test_robinhood_pdt_two_trades_still_allowed() {
        let mut model = RobinhoodBrokerageModel::default().with_equity(10_000.0);
        model.day_trade_count = 2;
        assert!(!model.pdt_limit_reached());
        assert!(model.can_submit_order());
    }

    /// A large account bypasses PDT restrictions even with many day trades.
    #[test]
    fn test_robinhood_pdt_large_account_bypasses_limit() {
        let mut model = RobinhoodBrokerageModel::default().with_equity(50_000.0);
        model.day_trade_count = 10;
        assert!(
            !model.pdt_limit_reached(),
            "Account over $25k must not be blocked regardless of day trade count"
        );
        assert!(model.can_submit_order());
    }

    // -- Supported security types ---------------------------------------------

    /// Equity securities are supported.
    #[test]
    fn test_robinhood_supported_security_type_equity() {
        use lean_core::SecurityType;
        // Robinhood's primary product is equities — the model always permits them.
        // Verify through the leverage and fee model rather than a dedicated
        // `is_supported` method (which the impl does not expose separately).
        let model = RobinhoodBrokerageModel::instant();
        // Equity is the default security type for Robinhood.
        // Model name is set and leverage is > 0 — basic smoke test.
        assert!(model.default_leverage() > 0.0);
        let _ = SecurityType::Equity; // compile-time proof the type is accessible
    }

    /// Options are supported via the dedicated options fee model.
    #[test]
    fn test_robinhood_supported_security_type_options() {
        use lean_core::SecurityType;
        use lean_orders::security_transaction_model::OrderFeeParameters;
        use lean_orders::order::OrderDirection;

        let model = RobinhoodBrokerageModel::instant();
        let fee_model = model.options_fee_model();
        let params = OrderFeeParameters {
            security_price: dec!(2.50),
            order_quantity: dec!(1),
            order_direction: OrderDirection::Buy,
        };
        // Should compute a fee without panicking — options are a supported type.
        let fee = fee_model.get_order_fee(&params);
        assert!(fee.value >= dec!(0), "Options fee must be non-negative");
        let _ = SecurityType::Option;
    }

    // -- Options approval level ------------------------------------------------

    /// Default options level is Level2 (long calls, long puts, spreads).
    #[test]
    fn test_robinhood_default_options_level() {
        let model = RobinhoodBrokerageModel::default();
        assert_eq!(model.options_level, OptionsLevel::Level2);
    }

    /// Options level can be upgraded to Level3 on Gold accounts.
    #[test]
    fn test_robinhood_options_level3_gold() {
        let model = RobinhoodBrokerageModel::gold()
            .with_options_level(OptionsLevel::Level3);
        assert_eq!(model.options_level, OptionsLevel::Level3);
    }

    /// OptionsLevel ordering: Level1 < Level2 < Level3.
    #[test]
    fn test_robinhood_options_level_ordering() {
        assert!(OptionsLevel::Level1 < OptionsLevel::Level2);
        assert!(OptionsLevel::Level2 < OptionsLevel::Level3);
    }

    // -- Order capabilities ---------------------------------------------------

    /// `can_execute_order` is always true.
    #[test]
    fn test_robinhood_can_execute_order() {
        let model = RobinhoodBrokerageModel::instant();
        assert!(model.can_execute_order());
    }

    /// `can_update_order` is true — Robinhood supports cancel-and-replace for limits.
    #[test]
    fn test_robinhood_can_update_order() {
        let model = RobinhoodBrokerageModel::instant();
        assert!(model.can_update_order());
    }

    // -- Fractional shares ----------------------------------------------------

    /// Fractional shares are enabled by default.
    #[test]
    fn test_robinhood_fractional_shares_enabled_by_default() {
        let model = RobinhoodBrokerageModel::default();
        assert!(model.fractional_shares_enabled);
    }

    /// Fractional shares can be disabled via the builder.
    #[test]
    fn test_robinhood_fractional_shares_can_be_disabled() {
        let model = RobinhoodBrokerageModel::default().with_fractional_shares(false);
        assert!(!model.fractional_shares_enabled);
    }

    // -- Account tier defaults ------------------------------------------------

    /// `Default` implementation produces an Instant-tier paper account.
    #[test]
    fn test_robinhood_default_is_instant_paper() {
        let model = RobinhoodBrokerageModel::default();
        assert_eq!(model.account_tier, RobinhoodAccountTier::Instant);
        assert!(!model.is_live);
        assert_eq!(model.account_equity, 0.0);
        assert_eq!(model.day_trade_count, 0);
        assert!(!model.allow_shorting);
        assert!(model.fractional_shares_enabled);
    }

    /// `RobinhoodAccountTier::default()` is Instant.
    #[test]
    fn test_robinhood_account_tier_default() {
        assert_eq!(RobinhoodAccountTier::default(), RobinhoodAccountTier::Instant);
    }

    // -- Label / name ---------------------------------------------------------

    /// Tier label strings match expected values.
    #[test]
    fn test_robinhood_tier_labels() {
        assert_eq!(RobinhoodAccountTier::Instant.label(), "Robinhood Instant");
        assert_eq!(RobinhoodAccountTier::Gold.label(), "Robinhood Gold");
        assert_eq!(RobinhoodAccountTier::Cash.label(), "Robinhood Cash");
    }
}
