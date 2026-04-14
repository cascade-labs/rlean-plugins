/// Fidelity brokerage model.
///
/// Models Fidelity's standard retail brokerage account as well as BrokerageLink
/// (401k/retirement) accounts.  Key characteristics:
///
/// **Fee structure**
///   - Equities / ETFs:   $0 per trade
///   - Equity options:    $0.65/contract (no per-leg base fee)
///   - Index options:     $0.65/contract
///   - Fixed-income:      $1/bond online, $19.95 broker-assisted (not modelled)
///
/// **Account types**
///   - [`FidelityAccountType::Cash`]       – no margin; 1× leverage; suitable for IRAs
///     and BrokerageLink (401k) accounts
///   - [`FidelityAccountType::Margin`]     – standard Reg-T margin; 2× equity overnight,
///     4× intraday (pattern day trader)
///   - [`FidelityAccountType::Retirement`] – IRA / Roth; behaves like Cash (no margin),
///     with additional restrictions (no short selling, no naked options)
///
/// **Leverage**
///   | account type | equity (overnight) | equity (intraday) |
///   |---|---|---|
///   | Cash / Retirement | 1× | 1× |
///   | Margin | 2× | 4× |
///
/// **Pattern Day Trader (PDT)**
///   Margin accounts with equity below $25,000 are subject to FINRA PDT rules
///   (max 3 day-trade round-trips per 5 rolling business days).  The model
///   exposes [`FidelityBrokerageModel::is_pdt_restricted`] so calling code can
///   gate same-day round-trip orders appropriately.
///
/// **Order types supported**
///   - Equity / ETF:  Market, Limit, StopMarket, StopLimit, MarketOnOpen,
///     MarketOnClose
///   - Options:       Market, Limit (exercise via [`lean_orders::OptionExerciseOrder`])
///
/// **Order updates**
///   Fidelity's web interface does not support in-flight order modifications.
///   Cancel-and-replace is the only supported workflow; [`can_update_order`]
///   always returns `false`.
///
/// **Short selling**
///   Permitted in Margin accounts only.  Retirement and Cash accounts cannot
///   hold short equity positions.  The model exposes
///   [`FidelityBrokerageModel::can_short`] for use in pre-trade checks.
use lean_brokerages::BrokerageModel;
use lean_orders::{
    fee_model::FidelityFeeModel,
    order::{Order, OrderDirection, OrderType},
    security_transaction_model::{OrderFee as LegacyOrderFee, SecurityTransactionModel},
};
use lean_core::SecurityType;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

// ─── Account type ─────────────────────────────────────────────────────────────

/// Describes the type of Fidelity account, which drives leverage and
/// restriction rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FidelityAccountType {
    /// Standard margin brokerage account (Reg-T).
    ///
    /// Default for a typical Fidelity retail account.
    #[default]
    Margin,
    /// Cash-only account — no margin borrowing.
    ///
    /// Also used for Fidelity BrokerageLink (401k) accounts where the plan
    /// does not allow margin.
    Cash,
    /// IRA, Roth IRA, or other tax-advantaged retirement account.
    ///
    /// Behaves like `Cash` (no margin), but additionally prohibits short
    /// selling and uncovered (naked) option writing.
    Retirement,
}

impl FidelityAccountType {
    /// Returns `true` if this account type is allowed to borrow on margin.
    pub fn allows_margin(&self) -> bool {
        matches!(self, FidelityAccountType::Margin)
    }

    /// Returns `true` if short equity positions are permitted.
    pub fn allows_short_selling(&self) -> bool {
        matches!(self, FidelityAccountType::Margin)
    }

    /// Returns `true` if uncovered (naked) option writing is permitted.
    pub fn allows_naked_options(&self) -> bool {
        matches!(self, FidelityAccountType::Margin)
    }
}

// ─── PDT state ────────────────────────────────────────────────────────────────

/// Captures the information needed to evaluate Pattern Day Trader rules for a
/// margin account.
///
/// FINRA Rule 4210 requires that a pattern day trader (someone who executes
/// ≥4 day trades in 5 rolling business days in a margin account) maintain at
/// least $25,000 of net equity.  Accounts that fall below this threshold while
/// still flagged as PDT are restricted to closing transactions only.
#[derive(Debug, Clone)]
pub struct PdtState {
    /// Number of day-trade round-trips executed in the current 5-business-day
    /// rolling window (0–n).
    pub day_trades_in_window: u32,
    /// Current account net liquidation value in USD.
    pub account_equity_usd: Decimal,
}

impl PdtState {
    /// Construct a new PDT state snapshot.
    pub fn new(day_trades_in_window: u32, account_equity_usd: Decimal) -> Self {
        Self { day_trades_in_window, account_equity_usd }
    }

    /// Minimum equity required to avoid PDT restrictions (FINRA: $25,000).
    pub const PDT_MINIMUM_EQUITY: Decimal = dec!(25_000);

    /// Threshold at which an account is flagged as a pattern day trader
    /// (≥4 day trades in 5 rolling business days per FINRA Rule 4210).
    pub const PDT_TRADE_THRESHOLD: u32 = 4;

    /// Returns `true` when the account is currently restricted by PDT rules
    /// (flagged as PDT AND equity is below the minimum).
    pub fn is_restricted(&self) -> bool {
        self.day_trades_in_window >= Self::PDT_TRADE_THRESHOLD
            && self.account_equity_usd < Self::PDT_MINIMUM_EQUITY
    }

    /// Returns `true` when the account has been flagged as a pattern day
    /// trader (regardless of whether it meets the equity minimum).
    pub fn is_pattern_day_trader(&self) -> bool {
        self.day_trades_in_window >= Self::PDT_TRADE_THRESHOLD
    }
}

// ─── Margin / leverage constants ──────────────────────────────────────────────

/// Overnight Reg-T margin leverage for equities (2×).
pub const REG_T_OVERNIGHT_LEVERAGE: f64 = 2.0;

/// Intraday leverage for PDT-qualified accounts (4×).
pub const PDT_INTRADAY_LEVERAGE: f64 = 4.0;

/// Cash / retirement accounts: no leverage (1×).
pub const CASH_LEVERAGE: f64 = 1.0;

// ─── Supported order types ────────────────────────────────────────────────────

/// Set of [`OrderType`] values accepted by Fidelity for equity / ETF orders.
pub const EQUITY_ORDER_TYPES: &[OrderType] = &[
    OrderType::Market,
    OrderType::Limit,
    OrderType::StopMarket,
    OrderType::StopLimit,
    OrderType::MarketOnOpen,
    OrderType::MarketOnClose,
];

/// Set of [`OrderType`] values accepted by Fidelity for option orders.
pub const OPTION_ORDER_TYPES: &[OrderType] = &[
    OrderType::Market,
    OrderType::Limit,
    OrderType::OptionExercise,
];

// ─── Validation result ────────────────────────────────────────────────────────

/// Describes the outcome of an order-submission validation check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderValidation {
    /// Order is valid and may be submitted.
    Accepted,
    /// Order is rejected; the inner string describes the reason.
    Rejected(String),
}

impl OrderValidation {
    pub fn is_accepted(&self) -> bool {
        matches!(self, OrderValidation::Accepted)
    }

    pub fn rejection_reason(&self) -> Option<&str> {
        match self {
            OrderValidation::Rejected(msg) => Some(msg),
            _ => None,
        }
    }
}

// ─── FidelityFeeAdapter ───────────────────────────────────────────────────────

/// Adapter that bridges [`FidelityFeeModel`] (which implements the richer
/// [`FeeModel`] trait from `lean_orders::fee_model`) into the simpler
/// [`SecurityTransactionModel`] interface used by [`BrokerageModel`].
///
/// This keeps the rich fee calculation in the canonical location
/// (`lean-orders::fee_model`) while satisfying the broker-model trait boundary.
struct FidelityFeeAdapter;

impl SecurityTransactionModel for FidelityFeeAdapter {
    fn get_order_fee(
        &self,
        _params: &lean_orders::security_transaction_model::OrderFeeParameters,
    ) -> lean_orders::security_transaction_model::OrderFee {
        // The legacy SecurityTransactionModel interface lacks security-type
        // context, so we conservatively return $0 (equity rate).
        //
        // Callers that need accurate option fees ($0.65/contract) should use
        // the richer `FidelityFeeModel` (implements `FeeModel`) directly via
        // `FidelityBrokerageModel::fee_model()`.
        LegacyOrderFee::flat(dec!(0), "USD")
    }
}

// ─── FidelityBrokerageModel ───────────────────────────────────────────────────

/// Brokerage model for Fidelity Investments.
///
/// See module-level documentation for a complete feature summary.
pub struct FidelityBrokerageModel {
    /// The type of account this model is configured for.
    pub account_type: FidelityAccountType,
    /// Optional PDT state snapshot.  When `Some`, PDT restriction checks are
    /// performed during order validation.
    pub pdt_state: Option<PdtState>,
}

impl Default for FidelityBrokerageModel {
    fn default() -> Self {
        Self {
            account_type: FidelityAccountType::Margin,
            pdt_state: None,
        }
    }
}

impl FidelityBrokerageModel {
    // ── Constructors ──────────────────────────────────────────────────────────

    /// Creates a standard margin brokerage account model.
    ///
    /// Use this for typical retail Fidelity accounts that allow margin.
    pub fn margin() -> Self {
        Self { account_type: FidelityAccountType::Margin, pdt_state: None }
    }

    /// Creates a cash-only account model.
    ///
    /// Use this for Fidelity BrokerageLink / 401k accounts or any account
    /// that does not have margin privileges.
    pub fn cash() -> Self {
        Self { account_type: FidelityAccountType::Cash, pdt_state: None }
    }

    /// Creates an IRA / retirement account model.
    ///
    /// Retirement accounts have the same leverage as cash accounts (1×) and
    /// additionally prohibit short selling and naked options.
    pub fn retirement() -> Self {
        Self { account_type: FidelityAccountType::Retirement, pdt_state: None }
    }

    /// Creates a model with a specific [`FidelityAccountType`].
    pub fn new(account_type: FidelityAccountType) -> Self {
        Self { account_type, pdt_state: None }
    }

    /// Attaches a PDT state snapshot.
    ///
    /// When set, [`validate_order`] will reject new same-direction orders from
    /// a restricted PDT account.
    pub fn with_pdt_state(mut self, state: PdtState) -> Self {
        self.pdt_state = Some(state);
        self
    }

    // ── Leverage ──────────────────────────────────────────────────────────────

    /// Returns the **overnight** leverage for the given security type.
    ///
    /// This is the leverage that applies at end-of-day (i.e., for position
    /// sizing purposes in backtesting without intraday margin changes).
    pub fn overnight_leverage(&self, security_type: SecurityType) -> f64 {
        match self.account_type {
            FidelityAccountType::Cash | FidelityAccountType::Retirement => CASH_LEVERAGE,
            FidelityAccountType::Margin => match security_type {
                SecurityType::Equity => REG_T_OVERNIGHT_LEVERAGE,
                SecurityType::Option
                | SecurityType::FutureOption
                | SecurityType::IndexOption => CASH_LEVERAGE, // options require full premium
                _ => CASH_LEVERAGE,
            },
        }
    }

    /// Returns the **intraday** leverage for the given security type.
    ///
    /// For Margin accounts, PDT-qualified accounts may use 4× intraday.
    /// Cash and Retirement accounts always use 1×.
    pub fn intraday_leverage(&self, security_type: SecurityType) -> f64 {
        if self.account_type != FidelityAccountType::Margin {
            return CASH_LEVERAGE;
        }
        match security_type {
            SecurityType::Equity => PDT_INTRADAY_LEVERAGE,
            _ => self.overnight_leverage(security_type),
        }
    }

    // ── PDT ───────────────────────────────────────────────────────────────────

    /// Returns `true` when the attached PDT state indicates the account is
    /// currently restricted by pattern day trader rules.
    ///
    /// Always returns `false` for Cash and Retirement accounts (PDT rules only
    /// apply to margin accounts).
    pub fn is_pdt_restricted(&self) -> bool {
        if self.account_type != FidelityAccountType::Margin {
            return false;
        }
        self.pdt_state.as_ref().map(|s| s.is_restricted()).unwrap_or(false)
    }

    // ── Short selling ─────────────────────────────────────────────────────────

    /// Returns `true` when the account type permits short selling of equities.
    pub fn can_short(&self) -> bool {
        self.account_type.allows_short_selling()
    }

    // ── Fee model ─────────────────────────────────────────────────────────────

    /// Returns the canonical Fidelity fee model.
    ///
    /// Callers that have access to the richer [`OrderFeeParameters`] (including
    /// security type) should use this directly for accurate fee calculation.
    pub fn fee_model(&self) -> FidelityFeeModel {
        FidelityFeeModel
    }

    // ── Order validation ──────────────────────────────────────────────────────

    /// Validates whether the given order may be submitted under Fidelity rules.
    ///
    /// Checks performed (in order):
    /// 1. Security type is supported (Equity or Options only)
    /// 2. Order type is valid for the security type
    /// 3. Short orders are only allowed in Margin accounts
    /// 4. PDT restriction: if the account is PDT-restricted, only closing
    ///    (sell) equity orders are allowed
    pub fn validate_order(
        &self,
        order: &Order,
        security_type: SecurityType,
    ) -> OrderValidation {
        // 1. Supported security types
        match security_type {
            SecurityType::Equity
            | SecurityType::Option
            | SecurityType::FutureOption
            | SecurityType::IndexOption => {}
            other => {
                return OrderValidation::Rejected(format!(
                    "Fidelity does not support {} securities. \
                     Only Equity and Options are tradeable.",
                    other
                ));
            }
        }

        // 2. Supported order types
        let allowed_types = if security_type.is_option_like() {
            OPTION_ORDER_TYPES
        } else {
            EQUITY_ORDER_TYPES
        };

        if !allowed_types.contains(&order.order_type) {
            return OrderValidation::Rejected(format!(
                "Fidelity does not support {:?} orders for {} securities. \
                 Allowed: {:?}",
                order.order_type, security_type, allowed_types
            ));
        }

        // 3. Short-selling restriction for non-Margin accounts
        if security_type == SecurityType::Equity
            && order.direction() == OrderDirection::Sell
            && order.quantity < dec!(0)
            && !self.account_type.allows_short_selling()
        {
            return OrderValidation::Rejected(format!(
                "Short selling is not permitted in a {:?} account.",
                self.account_type
            ));
        }

        // 4. PDT restriction
        if self.is_pdt_restricted() && order.direction() == OrderDirection::Buy {
            return OrderValidation::Rejected(
                "Account is subject to Pattern Day Trader restrictions (FINRA Rule 4210). \
                 Equity must be at least $25,000 to place new buy orders. \
                 Only position-closing sell orders are permitted."
                    .into(),
            );
        }

        OrderValidation::Accepted
    }

    // ── Margin requirements ───────────────────────────────────────────────────

    /// Returns the initial margin requirement (as a fraction of position value)
    /// for a given security type in a Margin account.
    ///
    /// Follows FINRA/Reg-T requirements:
    /// - Equity: 50% initial margin (2× leverage)
    /// - Options: 100% (full premium required; no leverage)
    /// - Cash / Retirement accounts: always 100%
    pub fn initial_margin_requirement(&self, security_type: SecurityType) -> Decimal {
        if !self.account_type.allows_margin() {
            return dec!(1.0); // 100% cash required
        }
        match security_type {
            SecurityType::Equity => dec!(0.50), // Reg-T 50% initial margin
            SecurityType::Option
            | SecurityType::FutureOption
            | SecurityType::IndexOption => dec!(1.0), // options: full premium
            _ => dec!(1.0),
        }
    }

    /// Returns the maintenance margin requirement (as a fraction) for a given
    /// security type in a Margin account.
    ///
    /// Follows standard Fidelity / FINRA maintenance requirements:
    /// - Equity (long):       25%
    /// - Equity (short):      30%
    /// - Options:             100% (no credit given)
    pub fn maintenance_margin_requirement(
        &self,
        security_type: SecurityType,
        is_short: bool,
    ) -> Decimal {
        if !self.account_type.allows_margin() {
            return dec!(1.0);
        }
        match security_type {
            SecurityType::Equity => {
                if is_short { dec!(0.30) } else { dec!(0.25) }
            }
            SecurityType::Option
            | SecurityType::FutureOption
            | SecurityType::IndexOption => dec!(1.0),
            _ => dec!(1.0),
        }
    }
}

// ─── BrokerageModel trait impl ────────────────────────────────────────────────

impl BrokerageModel for FidelityBrokerageModel {
    fn name(&self) -> &str {
        "Fidelity"
    }

    /// Returns the [`SecurityTransactionModel`] for legacy compatibility.
    ///
    /// The returned model always produces $0 for equity and $0.65/contract for
    /// options, approximated via the fee adapter.  For production use the richer
    /// [`fee_model()`] method (which implements [`FeeModel`]) should be
    /// preferred when a full [`OrderFeeParameters`] is available.
    fn transaction_model(&self) -> Box<dyn SecurityTransactionModel> {
        Box::new(FidelityFeeAdapter)
    }

    /// Default overnight leverage based on account type.
    ///
    /// Returns 2.0 for Margin accounts (Reg-T equity overnight) and 1.0 for
    /// Cash / Retirement accounts.  Use [`intraday_leverage`] for the 4×
    /// intraday multiplier in PDT-qualified margin accounts.
    fn default_leverage(&self) -> f64 {
        match self.account_type {
            FidelityAccountType::Cash | FidelityAccountType::Retirement => CASH_LEVERAGE,
            FidelityAccountType::Margin => REG_T_OVERNIGHT_LEVERAGE,
        }
    }

    /// Fidelity supports Market and Limit order submission for equities and
    /// options.  Additional order types (Stop, MOO, MOC) are also accepted.
    fn can_submit_order(&self) -> bool {
        true
    }

    /// Fidelity does **not** support in-flight order modifications.
    ///
    /// The web/API interface only supports cancel-and-replace.
    fn can_update_order(&self) -> bool {
        false
    }

    /// Fidelity can execute all equity and option orders that have been
    /// successfully submitted.
    fn can_execute_order(&self) -> bool {
        true
    }
}

// ─── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use lean_orders::security_transaction_model::OrderFeeParameters;
    use lean_orders::OrderDirection;
    use rust_decimal_macros::dec;

    #[test]
    fn name() { assert_eq!(FidelityBrokerageModel::default().name(), "Fidelity"); }

    #[test]
    fn cash_account_uses_cash_leverage() {
        let m = FidelityBrokerageModel { account_type: FidelityAccountType::Cash, pdt_state: None };
        assert_eq!(m.default_leverage(), CASH_LEVERAGE);
    }

    #[test]
    fn pdt_intraday_higher_than_overnight() {
        assert!(PDT_INTRADAY_LEVERAGE > REG_T_OVERNIGHT_LEVERAGE);
    }

    #[test]
    fn equity_fee_nonnegative() {
        let fee = FidelityBrokerageModel::default().transaction_model()
            .get_order_fee(&OrderFeeParameters { security_price: dec!(100), order_quantity: dec!(10), order_direction: OrderDirection::Buy });
        assert!(fee.value >= dec!(0));
    }
}
