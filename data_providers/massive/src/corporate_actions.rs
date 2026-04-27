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
use std::path::Path;

use anyhow::{Context, Result};
use chrono::NaiveDate;
use tracing::{info, warn};

use lean_storage::schema::{FactorFileEntry, MapFileEntry};
use lean_storage::{ParquetReader, ParquetWriter, WriterConfig};

use crate::models::{MassiveDividendItem, MassiveSplitItem};

// ─── Computation ─────────────────────────────────────────────────────────────

/// Build a list of `FactorFileEntry`s from raw Massive API data.
///
/// `ref_prices` – map of date → unadjusted close price (used to compute
///   the price factor for each dividend).
///
/// Returns rows sorted **newest first** (LEAN file ordering), with a sentinel
/// row at index 0 dated `today`.
pub fn compute_factor_rows(
    splits: &[MassiveSplitItem],
    dividends: &[MassiveDividendItem],
    ref_prices: &HashMap<NaiveDate, f64>,
    today: NaiveDate,
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

    // Sentinel row: today's date, all factors = 1.0, reference_price = 0.
    let mut rows: Vec<FactorFileEntry> = Vec::new();
    rows.push(FactorFileEntry {
        date: today,
        price_factor: 1.0,
        split_factor: 1.0,
        reference_price: 0.0,
    });

    let mut cum_price = 1.0_f64;
    let mut cum_split = 1.0_f64;

    for event in &events {
        match event {
            Event::Split { date, factor } => {
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
                    reference_price: 0.0,
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

    // Push a base row at a far-past date carrying the final cumulative factors.
    // This is the "bottom" of the factor file: rlean's backward-extension logic
    // returns the oldest row for any bar_date that predates all explicit rows.
    // Without this row, bars in the pre-split era fall back to the split boundary
    // row (sf=1.0) instead of the correct pre-split factor (e.g. sf=0.5).
    if (cum_price - 1.0).abs() > 1e-9 || (cum_split - 1.0).abs() > 1e-9 {
        let base_date = NaiveDate::from_ymd_opt(1900, 1, 1).unwrap();
        rows.push(FactorFileEntry {
            date: base_date,
            price_factor: cum_price,
            split_factor: cum_split,
            reference_price: 0.0,
        });
    }

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

// ─── Parquet I/O ─────────────────────────────────────────────────────────────

/// Write factor rows (newest first) to a Parquet file.
pub fn write_factor_file(path: &Path, rows: &[FactorFileEntry]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).context("create factor_files dir")?;
    }
    let writer = ParquetWriter::new(WriterConfig::default());
    writer
        .write_factor_file(rows, path)
        .map_err(|e| anyhow::anyhow!("{e}"))
}

/// Read factor rows (newest first) from a Parquet file.
/// Returns an empty Vec (no error) if the file doesn't exist.
pub fn read_factor_file(path: &Path) -> Result<Vec<FactorFileEntry>> {
    let reader = ParquetReader::new();
    reader
        .read_factor_file(path)
        .map_err(|e| anyhow::anyhow!("{e}"))
}

// ─── Massive fetch + write ────────────────────────────────────────────────────

/// Fetch splits + dividends from Massive and write a factor file for `symbol`.
///
/// `ref_prices` — map of date → unadjusted close (used to compute the dividend
/// price factor).  Typically built from already-downloaded bar data.
pub async fn fetch_and_write_factor_file(
    client: &crate::client::MassiveRestClient,
    factor_path: &Path,
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

    let today = chrono::Utc::now().date_naive();
    let rows = compute_factor_rows(&splits, &dividends, ref_prices, today);
    write_factor_file(factor_path, &rows)?;

    Ok(rows)
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
/// LEAN convention (Parquet, sorted newest first):
///   - Row 0 (newest): sentinel / delisting date with current ticker
///   - Row 1 ... N: historical ticker mappings (if any renames)
///
/// For a simple active ticker with no renames:
///   [{date: FAR_FUTURE, ticker: "XLK"}, {date: list_date, ticker: "XLK"}]
///
/// For a delisted ticker:
///   [{date: delisting_date, ticker: "META"}, ..., {date: list_date, ticker: "FB"}]
///
/// DelistingDate = first (newest) row's date if ≤ today + 1 year; else active.
pub fn compute_map_file_rows(
    ticker: &str,
    list_date: Option<NaiveDate>,
    delisting_date: Option<NaiveDate>,
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

    // Newest first (LEAN file ordering)
    let mut rows = vec![MapFileEntry {
        date: end_date,
        ticker: ticker_upper.clone(),
    }];
    if start_date < end_date {
        rows.push(MapFileEntry {
            date: start_date,
            ticker: ticker_upper,
        });
    }
    rows
}

/// Fetch ticker details and write a map file for `ticker`.
pub async fn fetch_and_write_map_file(
    client: &crate::client::MassiveRestClient,
    map_path: &Path,
    ticker: &str,
    today: NaiveDate,
) -> Result<Vec<MapFileEntry>> {
    info!("Massive: fetching ticker details for {}", ticker);

    let details = client
        .get_ticker_details(ticker)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let (list_date, delisting_date) = if let Some(d) = details {
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

    let rows = compute_map_file_rows(ticker, list_date, delisting_date, today);
    write_map_file(map_path, &rows)?;
    Ok(rows)
}

/// Write map file rows (newest first) to a Parquet file.
pub fn write_map_file(path: &Path, rows: &[MapFileEntry]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).context("create map_files dir")?;
    }
    let writer = ParquetWriter::new(WriterConfig::default());
    writer
        .write_map_file(rows, path)
        .map_err(|e| anyhow::anyhow!("{e}"))
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
    fn compute_factor_rows_sentinel_row_is_always_present() {
        let today = date(2024, 1, 1);
        let rows = compute_factor_rows(&[], &[], &HashMap::new(), today);
        assert!(!rows.is_empty());
        assert_eq!(rows[0].date, today);
        assert_eq!(rows[0].price_factor, 1.0);
        assert_eq!(rows[0].split_factor, 1.0);
    }

    #[test]
    fn factor_for_date_returns_one_one_when_no_rows() {
        assert_eq!(factor_for_date(&[], date(2023, 1, 1)), (1.0, 1.0));
    }

    #[test]
    fn factor_file_round_trip_with_multiple_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("aapl.parquet");

        let rows = vec![
            FactorFileEntry {
                date: date(2024, 1, 1),
                price_factor: 1.0,
                split_factor: 1.0,
                reference_price: 0.0,
            },
            FactorFileEntry {
                date: date(2022, 8, 31),
                price_factor: 1.0,
                split_factor: 0.25,
                reference_price: 150.0,
            },
        ];

        write_factor_file(&path, &rows).unwrap();
        assert!(path.exists());

        let read_back = read_factor_file(&path).unwrap();
        assert_eq!(read_back.len(), rows.len());
        for (orig, got) in rows.iter().zip(read_back.iter()) {
            assert_eq!(orig.date, got.date);
            assert!((orig.split_factor - got.split_factor).abs() < 1e-9);
        }
    }
}
