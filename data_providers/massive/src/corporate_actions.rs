/// Corporate actions (splits + dividends) → LEAN-compatible factor files.
///
/// Factor file format (Parquet), sorted newest → oldest:
///   date_ns: i64 (ns since Unix epoch), price_factor: f64, split_factor: f64, reference_price: f64
///
/// Factors are applied to raw (unadjusted) prices as:
///   adjustedPrice = rawPrice * priceFactor * splitFactor
///
/// The sentinel row (newest date, factors = 1.0) marks when the file was
/// last verified and enables incremental updates identical to the C# provider.
use std::collections::HashMap;

use anyhow::Result;
use chrono::NaiveDate;
use tracing::{info, warn};

use lean_storage::schema::{FactorFileEntry, MapFileEntry};

use crate::models::{MassiveDividendItem, MassiveSplitItem, TickerEvent};

pub(crate) fn lean_factor_file_end_date() -> NaiveDate {
    NaiveDate::from_ymd_opt(2050, 12, 31).unwrap()
}

pub(crate) fn lean_factor_file_start_date() -> NaiveDate {
    NaiveDate::from_ymd_opt(1900, 1, 1).unwrap()
}

/// Earliest date Massive's aggregates (bars) endpoint accepts.
///
/// The factor file spans back to `lean_factor_file_start_date()` (1900), but the
/// bars API rejects pre-epoch dates: a 1900 `from` becomes a **negative** Unix
/// timestamp and returns `{"status":"ERROR","error":"Could not parse the time
/// parameter: 'from'"}` with no rows. That silently emptied the dividend
/// reference-price map, so every dividend was skipped and `price_factor`
/// collapsed to 1.0. Reference prices only need to cover the dividends we can
/// price, so clamp the bars fetch to Polygon/Massive's equities inception.
pub(crate) fn massive_aggregates_floor_date() -> NaiveDate {
    NaiveDate::from_ymd_opt(2003, 9, 10).unwrap()
}

// ─── Computation ─────────────────────────────────────────────────────────────

/// Build a list of `FactorFileEntry`s from raw Massive API data.
///
/// `ref_prices` – map of date → unadjusted close price (used to compute
///   the price factor for each dividend).
///
/// Returns rows sorted **newest first** (LEAN file ordering), with a sentinel
/// row at index 0 dated `sentinel_date`.
pub fn compute_factor_rows(
    splits: &[MassiveSplitItem],
    dividends: &[MassiveDividendItem],
    ref_prices: &HashMap<NaiveDate, f64>,
    sentinel_date: NaiveDate,
) -> Vec<FactorFileEntry> {
    #[derive(Debug)]
    enum Event {
        Split { date: NaiveDate, factor: f64 },
        Dividend { date: NaiveDate, amount: f64 },
    }

    let mut events: Vec<Event> = Vec::new();

    for s in splits {
        if let Ok(d) = NaiveDate::parse_from_str(&s.execution_date, "%Y-%m-%d") {
            let f = s.split_factor();
            // rlean runner uses max(date < bar_date) to look up factors.  A split
            // row placed at `execution_date` (the ex-date) would first apply to
            // bars on execution_date+1, leaving the ex-date bar with the wrong
            // (pre-split) factor.  Shift the row one calendar day back so it
            // applies starting on the ex-date itself.
            if f != 0.0 {
                events.push(Event::Split {
                    date: d - chrono::Duration::days(1),
                    factor: f,
                });
            }
        }
    }
    for div in dividends {
        if let Ok(d) = NaiveDate::parse_from_str(&div.ex_dividend_date, "%Y-%m-%d") {
            if div.cash_amount > 0.0 {
                events.push(Event::Dividend {
                    date: d,
                    amount: div.cash_amount,
                });
            }
        }
    }

    events.sort_by(|a, b| {
        let da = match a {
            Event::Split { date, .. } | Event::Dividend { date, .. } => date,
        };
        let db = match b {
            Event::Split { date, .. } | Event::Dividend { date, .. } => date,
        };
        db.cmp(da)
    });

    // LEAN factor files end with a future sentinel row (usually 2050-12-31),
    // not the wall-clock date the file was generated.
    let mut rows: Vec<FactorFileEntry> = Vec::new();
    rows.push(FactorFileEntry {
        date: sentinel_date,
        price_factor: 1.0,
        split_factor: 1.0,
        reference_price: 0.0,
    });

    let mut cum_price = 1.0_f64;
    let mut cum_split = 1.0_f64;

    for event in &events {
        match event {
            Event::Split { date, factor } => {
                let reference_price =
                    find_prev_close(*date + chrono::Duration::days(1), ref_prices);
                // Push the boundary row BEFORE updating cum_split.
                //
                // Rows are processed newest → oldest.  The boundary row at
                // (ex_date - 1) marks the start of the post-split era for
                // rlean's `max(date < bar_date)` look-up: bars from ex_date
                // onwards use THIS row (current cum_split = post-split factor),
                // while bars before ex_date use the NEXT (older) row that will
                // be generated after we multiply cum_split by the split factor.
                rows.push(FactorFileEntry {
                    date: *date,
                    price_factor: cum_price,
                    split_factor: cum_split,
                    reference_price,
                });
                cum_split *= factor;
            }
            Event::Dividend { date, amount } => {
                let prev_close = find_prev_close(*date, ref_prices);
                if prev_close > 0.0 {
                    let factor = (prev_close - amount) / prev_close;
                    cum_price *= factor;
                    rows.push(FactorFileEntry {
                        date: *date,
                        price_factor: cum_price,
                        split_factor: cum_split,
                        reference_price: prev_close,
                    });
                } else {
                    warn!(
                        "FactorFile: no reference price for dividend on {} (amount={:.4}), skipping",
                        date, amount
                    );
                }
            }
        }
    }

    // Always write a far-past base row. For symbols with no actions this is an
    // identity row, but it still proves the file covers historical backtests.
    rows.push(FactorFileEntry {
        date: lean_factor_file_start_date(),
        price_factor: cum_price,
        split_factor: cum_split,
        reference_price: 0.0,
    });

    rows
}

fn find_prev_close(event_date: NaiveDate, prices: &HashMap<NaiveDate, f64>) -> f64 {
    for i in 1..=5 {
        let d = event_date - chrono::Duration::days(i);
        if let Some(&p) = prices.get(&d) {
            return p;
        }
    }
    0.0
}

// ─── Massive fetch (pure data source) ─────────────────────────────────────────

/// Fetch splits + dividends from Massive and compute factor rows for `ticker`.
///
/// This is a pure data source: it returns the computed rows and performs **no**
/// persistence. The rlean framework owns all writes (into Iceberg), exactly as
/// it does for trade/quote bars.
///
/// `ref_prices` — map of date → unadjusted close (used to compute the dividend
/// price factor). Typically built from already-downloaded bar data.
pub async fn fetch_factor_rows(
    client: &crate::client::MassiveRestClient,
    ticker: &str,
    start: NaiveDate,
    end: NaiveDate,
    ref_prices: &HashMap<NaiveDate, f64>,
) -> Result<Vec<FactorFileEntry>> {
    info!("Massive: fetching corporate actions for {}", ticker);

    let splits = client.get_splits(ticker, start, end).await?;
    let dividends = client.get_dividends(ticker, start, end).await?;

    info!(
        "Massive: {} splits, {} dividends for {}",
        splits.len(),
        dividends.len(),
        ticker
    );

    Ok(compute_factor_rows(
        &splits,
        &dividends,
        ref_prices,
        lean_factor_file_end_date(),
    ))
}

// ─── Apply factors ────────────────────────────────────────────────────────────

/// Given factor rows (newest first) and a bar date, return `(price_factor, split_factor)`.
pub fn factor_for_date(rows: &[FactorFileEntry], bar_date: NaiveDate) -> (f64, f64) {
    if rows.is_empty() {
        return (1.0, 1.0);
    }

    let mut best: Option<&FactorFileEntry> = None;
    for r in rows {
        if r.date > bar_date {
            match best {
                None => best = Some(r),
                Some(b) if r.date < b.date => best = Some(r),
                _ => {}
            }
        }
    }

    match best {
        Some(r) => (r.price_factor, r.split_factor),
        None => (1.0, 1.0),
    }
}

// ─── Map file ────────────────────────────────────────────────────────────────

/// Build map file rows from ticker details.
///
/// LEAN convention (Parquet, sorted oldest first):
///   - Row 0: listing / first known date with the first ticker
///   - Intermediate rows: last date the previous ticker was valid
///   - Last row: sentinel / delisting date with current ticker
///
/// For a simple active ticker with no renames:
///   [{date: list_date, ticker: "XLK"}, {date: FAR_FUTURE, ticker: "XLK"}]
///
/// For a delisted ticker:
///   [{date: list_date, ticker: "FB"}, ..., {date: delisting_date, ticker: "META"}]
///
/// DelistingDate = last row's date if ≤ today + 1 year; else active.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TickerChangeEvent {
    /// First date the new ticker is effective.
    pub effective_date: NaiveDate,
    pub old_ticker: String,
    pub new_ticker: String,
}

pub fn compute_map_file_rows(
    ticker: &str,
    list_date: Option<NaiveDate>,
    delisting_date: Option<NaiveDate>,
    ticker_changes: &[TickerChangeEvent],
    _today: NaiveDate,
) -> Vec<MapFileEntry> {
    let ticker_upper = ticker.to_uppercase();

    // Sentinel / last-active date
    let end_date = delisting_date.unwrap_or_else(|| {
        // Active: use far-future sentinel (20501231 by LEAN convention)
        NaiveDate::from_ymd_opt(2050, 12, 31).unwrap()
    });

    let start_date = list_date.unwrap_or_else(|| {
        // Unknown listing date: use a safe fallback
        NaiveDate::from_ymd_opt(1998, 1, 2).unwrap()
    });

    let mut changes = ticker_changes
        .iter()
        .filter(|ev| ev.effective_date >= start_date && ev.effective_date <= end_date)
        .cloned()
        .collect::<Vec<_>>();
    changes.sort_by_key(|ev| ev.effective_date);

    let mut rows = Vec::new();
    rows.push(MapFileEntry {
        date: start_date,
        ticker: changes
            .first()
            .map(|ev| ev.old_ticker.to_uppercase())
            .unwrap_or_else(|| ticker_upper.clone()),
    });

    for change in &changes {
        let last_old_date = change
            .effective_date
            .checked_sub_signed(chrono::Duration::days(1))
            .unwrap_or(change.effective_date);
        if last_old_date >= start_date {
            rows.push(MapFileEntry {
                date: last_old_date,
                ticker: change.old_ticker.to_uppercase(),
            });
        }
    }

    if start_date < end_date {
        rows.push(MapFileEntry {
            date: end_date,
            ticker: ticker_upper,
        });
    }
    rows.sort_by_key(|r| r.date);
    rows.dedup();
    rows
}

fn ticker_event_date(event: &TickerEvent) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(
        event.date.split('T').next().unwrap_or(&event.date),
        "%Y-%m-%d",
    )
    .ok()
}

fn parse_ticker_change_event(
    previous_ticker: &str,
    event: &TickerEvent,
) -> Option<TickerChangeEvent> {
    let effective_date = ticker_event_date(event)?;
    let old_ticker = previous_ticker.to_uppercase();
    let new_ticker = event.ticker_change.ticker.to_uppercase();
    if old_ticker == new_ticker {
        return None;
    }
    Some(TickerChangeEvent {
        effective_date,
        old_ticker,
        new_ticker,
    })
}

/// Fetch ticker details and compute map file rows for `ticker`.
///
/// Pure data source: returns rows, performs no persistence. The rlean framework
/// owns the write into the Iceberg `map_files` table.
pub async fn fetch_map_rows(
    client: &crate::client::MassiveRestClient,
    ticker: &str,
    today: NaiveDate,
) -> Result<Vec<MapFileEntry>> {
    info!("Massive: fetching ticker details for {}", ticker);

    let details = client
        .get_ticker_details(ticker)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let (mut list_date, delisting_date) = if let Some(d) = details {
        let ld = d
            .list_date
            .as_deref()
            .and_then(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok());
        let dd = d.delisted_utc.as_deref().and_then(|s| {
            // delisted_utc may be "YYYY-MM-DDTHH:MM:SSZ" or "YYYY-MM-DD"
            let date_part = s.split('T').next().unwrap_or(s);
            NaiveDate::parse_from_str(date_part, "%Y-%m-%d").ok()
        });
        (ld, dd)
    } else {
        (None, None)
    };

    // Issue #29: a failed ticker-events fetch must NOT be swallowed and turned
    // into a default map. The engine persists any non-empty success and never
    // refetches, so a default map built after an error would permanently erase
    // a ticker's rename history (e.g. FB -> META) from the cache. Return the
    // error so the framework persists nothing and retries next run. We only
    // build a default map when the events fetch genuinely succeeds and there
    // are no rename events (the correct "no renames" case).
    let events = client
        .get_ticker_events(ticker)
        .await
        .map_err(|e| anyhow::anyhow!("Massive: could not fetch ticker events for {ticker}: {e}"))?;
    let (events_list_date, ticker_changes) = assemble_ticker_changes(&events);
    list_date = list_date.or(events_list_date);

    // Surface an unknown listing date. If neither ticker details nor the events
    // supplied a list date, `compute_map_file_rows` falls back to 1998-01-02,
    // which is a guess — make that visible so a wrong inception date is not
    // silent. Note: `get_ticker_details` returning None (a 404) is treated as a
    // legitimate "no reference record" rather than an error, because Massive
    // 404s for obscure/long-delisted tickers we still need to map; the events
    // fetch above still runs and can supply both the list date and any renames.
    if list_date.is_none() {
        tracing::warn!(
            "Massive: no listing date for {ticker}; using 1998-01-02 fallback \
             (details missing and no ticker-change events)"
        );
    }

    let rows = compute_map_file_rows(ticker, list_date, delisting_date, &ticker_changes, today);
    Ok(rows)
}

/// Turn raw ticker events into an ordered list of rename events, plus the
/// listing date implied by the first event (if any).
///
/// Pure so the issue #29 behavior is directly testable: on a genuine "no rename
/// events" input this yields `(None, [])`, and `fetch_map_rows` builds the
/// correct default map only in that case. It is never reached when the events
/// fetch errors, because `fetch_map_rows` returns that error first.
fn assemble_ticker_changes(events: &[TickerEvent]) -> (Option<NaiveDate>, Vec<TickerChangeEvent>) {
    let mut events: Vec<&TickerEvent> = events.iter().collect();
    events.sort_by(|a, b| a.date.cmp(&b.date));
    events.retain(|ev| ev.event_type == "ticker_change");

    let mut list_date = None;
    let mut ticker_changes = Vec::new();
    if let Some(first_event) = events.first() {
        list_date = ticker_event_date(first_event);
        let mut previous_ticker = first_event.ticker_change.ticker.to_uppercase();
        for event in events.iter().skip(1) {
            if let Some(change) = parse_ticker_change_event(&previous_ticker, event) {
                previous_ticker = change.new_ticker.clone();
                ticker_changes.push(change);
            }
        }
    }
    (list_date, ticker_changes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn date(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    /// Split rows must be placed at execution_date - 1 so that rlean's
    /// `max(date < bar_date)` look-up selects the correct factor on the
    /// first post-split bar (execution_date itself).
    ///
    /// Also verifies that a base row (sf=0.5 for a 2-for-1 split) is present
    /// at a far-past date so pre-split bars are correctly halved.
    #[test]
    fn split_row_is_placed_at_ex_date_minus_one() {
        let ex_date = date(2025, 12, 5); // Friday — first post-split trading day
        let expected_row_date = date(2025, 12, 4); // Thursday

        let split = crate::models::MassiveSplitItem {
            ticker: "XLK".to_string(),
            execution_date: "2025-12-05".to_string(),
            split_from: 1.0,
            split_to: 2.0,
        };
        let today = date(2026, 1, 1);
        let rows = compute_factor_rows(&[split], &[], &HashMap::new(), today);

        // Find the boundary row at ex_date-1.
        let split_row = rows
            .iter()
            .find(|r| r.date == expected_row_date)
            .expect("split boundary row at ex_date-1 must exist");

        assert_eq!(
            split_row.date, expected_row_date,
            "split row date should be ex_date-1={} so rlean runner applies it starting on ex_date={}",
            expected_row_date, ex_date
        );
        // The boundary row must carry sf=1.0 (post-split era, no adjustment).
        assert!(
            (split_row.split_factor - 1.0).abs() < 1e-9,
            "split boundary row split_factor should be 1.0 (post-split era), got {}",
            split_row.split_factor
        );

        // A base row must exist with sf=0.5 (pre-split era, halve raw prices).
        let base_row = rows
            .iter()
            .min_by_key(|r| r.date)
            .expect("at least one row must exist");
        assert!(
            (base_row.split_factor - 0.5).abs() < 1e-9,
            "base row split_factor should be 0.5 (pre-split era), got {}",
            base_row.split_factor
        );
    }

    #[test]
    fn reverse_split_uses_lean_old_over_new_factor() {
        let split = crate::models::MassiveSplitItem {
            ticker: "DPST".to_string(),
            execution_date: "2023-06-05".to_string(),
            split_from: 10.0,
            split_to: 1.0,
        };

        let rows = compute_factor_rows(&[split], &[], &HashMap::new(), date(2026, 1, 1));
        let boundary = rows
            .iter()
            .find(|row| row.date == date(2023, 6, 4))
            .expect("boundary row should be the day before the split");
        let base = rows
            .iter()
            .min_by_key(|row| row.date)
            .expect("base row should exist");

        assert_eq!(boundary.split_factor, 1.0);
        assert_eq!(base.split_factor, 10.0);
    }

    #[test]
    fn map_rows_follow_lean_boundaries_for_ticker_rename() {
        let rows = compute_map_file_rows(
            "META",
            Some(date(2012, 5, 18)),
            None,
            &[TickerChangeEvent {
                effective_date: date(2022, 6, 9),
                old_ticker: "FB".to_string(),
                new_ticker: "META".to_string(),
            }],
            date(2026, 4, 28),
        );

        assert_eq!(
            rows,
            vec![
                MapFileEntry {
                    date: date(2012, 5, 18),
                    ticker: "FB".to_string()
                },
                MapFileEntry {
                    date: date(2022, 6, 8),
                    ticker: "FB".to_string()
                },
                MapFileEntry {
                    date: date(2050, 12, 31),
                    ticker: "META".to_string()
                }
            ]
        );
    }

    fn ticker_change_event(date: &str, new_ticker: &str) -> TickerEvent {
        TickerEvent {
            date: date.to_string(),
            event_type: "ticker_change".to_string(),
            ticker_change: crate::models::TickerChange {
                ticker: new_ticker.to_string(),
            },
        }
    }

    /// Issue #29: with a genuine "no rename events" result (the events fetch
    /// succeeded and returned nothing), `assemble_ticker_changes` yields no
    /// changes and no list date, so `fetch_map_rows` correctly builds a default
    /// identity map. This is the ONLY case where a default map is legitimate.
    #[test]
    fn no_events_yields_identity_map_with_no_renames() {
        let (list_date, changes) = assemble_ticker_changes(&[]);
        assert_eq!(list_date, None, "no events implies no listing date");
        assert!(changes.is_empty(), "no events must produce no renames");

        // The map built from that empty result is a clean identity map for the
        // ticker (fallback list date, far-future sentinel) — no fake renames.
        let rows = compute_map_file_rows("SPY", list_date, None, &changes, date(2026, 4, 28));
        assert_eq!(
            rows,
            vec![
                MapFileEntry {
                    date: date(1998, 1, 2),
                    ticker: "SPY".to_string()
                },
                MapFileEntry {
                    date: date(2050, 12, 31),
                    ticker: "SPY".to_string()
                }
            ]
        );
    }

    /// A real rename sequence is turned into ordered `TickerChangeEvent`s, and
    /// the first event supplies the listing date. Guards that the refactor into
    /// `assemble_ticker_changes` preserves the FB -> META mapping behavior.
    #[test]
    fn events_are_assembled_into_ordered_renames() {
        // First event is the listing under the original ticker; the second is
        // the rename to the new ticker.
        let events = vec![
            ticker_change_event("2012-05-18", "FB"),
            ticker_change_event("2022-06-09", "META"),
        ];
        let (list_date, changes) = assemble_ticker_changes(&events);
        assert_eq!(list_date, Some(date(2012, 5, 18)));
        assert_eq!(
            changes,
            vec![TickerChangeEvent {
                effective_date: date(2022, 6, 9),
                old_ticker: "FB".to_string(),
                new_ticker: "META".to_string(),
            }]
        );

        let rows = compute_map_file_rows("META", list_date, None, &changes, date(2026, 4, 28));
        assert_eq!(
            rows,
            vec![
                MapFileEntry {
                    date: date(2012, 5, 18),
                    ticker: "FB".to_string()
                },
                MapFileEntry {
                    date: date(2022, 6, 8),
                    ticker: "FB".to_string()
                },
                MapFileEntry {
                    date: date(2050, 12, 31),
                    ticker: "META".to_string()
                }
            ]
        );
    }

    #[test]
    fn compute_factor_rows_sentinel_row_is_always_present() {
        let rows = compute_factor_rows(&[], &[], &HashMap::new(), lean_factor_file_end_date());
        assert!(!rows.is_empty());
        assert_eq!(rows[0].date, lean_factor_file_end_date());
        assert_eq!(rows[0].price_factor, 1.0);
        assert_eq!(rows[0].split_factor, 1.0);
    }

    #[test]
    fn compute_factor_rows_writes_identity_base_for_no_action_symbols() {
        let rows = compute_factor_rows(&[], &[], &HashMap::new(), lean_factor_file_end_date());

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].date, lean_factor_file_end_date());
        assert_eq!(rows[1].date, lean_factor_file_start_date());
        assert_eq!(rows[1].price_factor, 1.0);
        assert_eq!(rows[1].split_factor, 1.0);
    }

    #[test]
    fn factor_for_date_returns_one_one_when_no_rows() {
        assert_eq!(factor_for_date(&[], date(2023, 1, 1)), (1.0, 1.0));
    }

    /// A cash dividend with a known reference close must produce a price factor
    /// below 1.0 for pre-ex-date bars. Regression guard for the bug where an
    /// empty `ref_prices` map silently collapsed every dividend to pf=1.0.
    #[test]
    fn dividend_with_reference_price_lowers_price_factor() {
        let ex = date(2022, 2, 28);
        // Close the day before the ex-date used to price the dividend.
        let mut prices = HashMap::new();
        prices.insert(ex - chrono::Duration::days(1), 20.0);

        let dividend = crate::models::MassiveDividendItem {
            ex_dividend_date: "2022-02-28".to_string(),
            cash_amount: 0.20,
            ticker: "KEY".to_string(),
            dividend_type: "CD".to_string(),
        };

        let rows = compute_factor_rows(&[], &[dividend], &prices, lean_factor_file_end_date());

        // Bars on/after the ex-date see pf=1.0 (sentinel era); bars before it
        // must be scaled by (20 - 0.20)/20 = 0.99.
        let (pf_before, _) = factor_for_date(&rows, ex - chrono::Duration::days(1));
        assert!(
            (pf_before - 0.99).abs() < 1e-9,
            "pre-ex-date price factor should be 0.99, got {pf_before}"
        );
        assert!(
            pf_before < 1.0,
            "dividend must lower the pre-ex-date price factor below 1.0"
        );
    }

    /// Without reference prices the dividend is skipped (documents the failure
    /// mode the history-provider clamp now prevents by supplying real closes).
    #[test]
    fn dividend_without_reference_price_is_skipped() {
        let dividend = crate::models::MassiveDividendItem {
            ex_dividend_date: "2022-02-28".to_string(),
            cash_amount: 0.20,
            ticker: "KEY".to_string(),
            dividend_type: "CD".to_string(),
        };
        let rows = compute_factor_rows(
            &[],
            &[dividend],
            &HashMap::new(),
            lean_factor_file_end_date(),
        );
        assert!(
            rows.iter().all(|r| (r.price_factor - 1.0).abs() < 1e-9),
            "with no reference prices all price factors stay 1.0 (bug condition)"
        );
    }
}
