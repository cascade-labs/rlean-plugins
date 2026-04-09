/// Corporate actions (splits + dividends) → LEAN-compatible factor files.
///
/// Factor file format (Parquet), sorted newest → oldest:
///   date_int: i32 (YYYYMMDD), price_factor: f64, split_factor: f64, dividend_amount: f64
///
/// Factors are applied to raw (unadjusted) prices as:
///   adjustedPrice = rawPrice * priceFactor * splitFactor
///
/// The sentinel row (newest date, factors = 1.0) marks when the file was
/// last verified and enables incremental updates identical to the C# provider.
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::{Float64Array, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use chrono::NaiveDate;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tracing::{debug, info, warn};

use crate::models::{MassiveDividendItem, MassiveSplitItem};

// ─── Data types ──────────────────────────────────────────────────────────────

/// One row of a LEAN factor file.
#[derive(Debug, Clone)]
pub struct FactorRow {
    /// The date from which (going backward) these cumulative factors apply.
    pub date: NaiveDate,
    /// Cumulative price-adjustment factor (dividends).
    pub price_factor: f64,
    /// Cumulative split factor.
    pub split_factor: f64,
    /// Raw dividend cash amount on this date (informational only).
    pub dividend_amount: f64,
}

// ─── Computation ─────────────────────────────────────────────────────────────

/// Build a list of `FactorRow`s from raw Massive API data.
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
) -> Vec<FactorRow> {
    #[derive(Debug)]
    enum Event {
        Split { date: NaiveDate, factor: f64 },
        Dividend { date: NaiveDate, amount: f64 },
    }

    let mut events: Vec<Event> = Vec::new();

    for s in splits {
        if let Ok(d) = NaiveDate::parse_from_str(&s.execution_date, "%Y-%m-%d") {
            let f = s.split_factor();
            if f != 0.0 { events.push(Event::Split { date: d, factor: f }); }
        }
    }
    for div in dividends {
        if let Ok(d) = NaiveDate::parse_from_str(&div.ex_dividend_date, "%Y-%m-%d") {
            if div.cash_amount > 0.0 { events.push(Event::Dividend { date: d, amount: div.cash_amount }); }
        }
    }

    events.sort_by(|a, b| {
        let da = match a { Event::Split { date, .. } | Event::Dividend { date, .. } => date };
        let db = match b { Event::Split { date, .. } | Event::Dividend { date, .. } => date };
        db.cmp(da)
    });

    let mut rows: Vec<FactorRow> = Vec::new();
    rows.push(FactorRow { date: today, price_factor: 1.0, split_factor: 1.0, dividend_amount: 0.0 });

    let mut cum_price = 1.0_f64;
    let mut cum_split = 1.0_f64;

    for event in &events {
        match event {
            Event::Split { date, factor } => {
                cum_split *= factor;
                rows.push(FactorRow {
                    date: *date,
                    price_factor: cum_price,
                    split_factor: cum_split,
                    dividend_amount: 0.0,
                });
            }
            Event::Dividend { date, amount } => {
                let prev_close = find_prev_close(*date, ref_prices);
                if prev_close > 0.0 {
                    let factor = (prev_close - amount) / prev_close;
                    cum_price *= factor;
                    rows.push(FactorRow {
                        date: *date,
                        price_factor: cum_price,
                        split_factor: cum_split,
                        dividend_amount: *amount,
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

    rows
}

fn find_prev_close(event_date: NaiveDate, prices: &HashMap<NaiveDate, f64>) -> f64 {
    for i in 1..=5 {
        let d = event_date - chrono::Duration::days(i);
        if let Some(&p) = prices.get(&d) { return p; }
    }
    0.0
}

// ─── Parquet I/O ─────────────────────────────────────────────────────────────

fn factor_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("date_int",        DataType::Int32,   false),
        Field::new("price_factor",    DataType::Float64, false),
        Field::new("split_factor",    DataType::Float64, false),
        Field::new("dividend_amount", DataType::Float64, false),
    ]))
}

/// Write factor rows (newest first) to a Parquet file.
pub fn write_factor_file(path: &Path, rows: &[FactorRow]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).context("create factor_files dir")?;
    }

    let schema = factor_schema();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let file = std::fs::File::create(path).context("create factor parquet")?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
        .context("open ArrowWriter")?;

    let dates:      Vec<i32> = rows.iter().map(|r| date_to_int(r.date)).collect();
    let prices:     Vec<f64> = rows.iter().map(|r| r.price_factor).collect();
    let splits:     Vec<f64> = rows.iter().map(|r| r.split_factor).collect();
    let dividends:  Vec<f64> = rows.iter().map(|r| r.dividend_amount).collect();

    let batch = RecordBatch::try_new(schema, vec![
        Arc::new(Int32Array::from(dates)),
        Arc::new(Float64Array::from(prices)),
        Arc::new(Float64Array::from(splits)),
        Arc::new(Float64Array::from(dividends)),
    ]).context("build factor RecordBatch")?;

    writer.write(&batch).context("write factor batch")?;
    writer.close().context("close factor parquet")?;

    debug!("FactorFile: wrote {} rows → {}", rows.len(), path.display());
    Ok(())
}

/// Read factor rows (newest first) from a Parquet file.
/// Returns an empty Vec (no error) if the file doesn't exist.
pub fn read_factor_file(path: &Path) -> Result<Vec<FactorRow>> {
    if !path.exists() { return Ok(Vec::new()); }

    let file = std::fs::File::open(path).context("open factor parquet")?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .context("open ParquetRecordBatchReaderBuilder")?;
    let mut reader = builder.build().context("build parquet reader")?;

    let mut rows = Vec::new();
    while let Some(batch) = reader.next() {
        let batch = batch.context("read factor batch")?;
        let dates     = batch.column(0).as_any().downcast_ref::<Int32Array>().context("date_int col")?;
        let prices    = batch.column(1).as_any().downcast_ref::<Float64Array>().context("price_factor col")?;
        let splits    = batch.column(2).as_any().downcast_ref::<Float64Array>().context("split_factor col")?;
        let dividends = batch.column(3).as_any().downcast_ref::<Float64Array>().context("dividend_amount col")?;

        for i in 0..batch.num_rows() {
            if let Some(date) = int_to_date(dates.value(i)) {
                rows.push(FactorRow {
                    date,
                    price_factor:    prices.value(i),
                    split_factor:    splits.value(i),
                    dividend_amount: dividends.value(i),
                });
            }
        }
    }
    Ok(rows)
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
) -> Result<Vec<FactorRow>> {
    info!("Massive: fetching corporate actions for {}", ticker);

    let splits    = client.get_splits(ticker, start, end).await?;
    let dividends = client.get_dividends(ticker, start, end).await?;

    info!(
        "Massive: {} splits, {} dividends for {}",
        splits.len(), dividends.len(), ticker
    );

    let today = chrono::Utc::now().date_naive();
    let rows = compute_factor_rows(&splits, &dividends, ref_prices, today);
    write_factor_file(factor_path, &rows)?;

    Ok(rows)
}

// ─── Apply factors ────────────────────────────────────────────────────────────

/// Given factor rows (newest first) and a bar date, return `(price_factor, split_factor)`.
pub fn factor_for_date(rows: &[FactorRow], bar_date: NaiveDate) -> (f64, f64) {
    if rows.is_empty() { return (1.0, 1.0); }

    let mut best: Option<&FactorRow> = None;
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
        None    => (1.0, 1.0),
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn date_to_int(d: NaiveDate) -> i32 {
    d.format("%Y%m%d").to_string().parse::<i32>().unwrap_or(0)
}

fn int_to_date(n: i32) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(&n.to_string(), "%Y%m%d").ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn date(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
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
    fn date_int_round_trip() {
        let d = date(2021, 8, 31);
        assert_eq!(int_to_date(date_to_int(d)), Some(d));
    }

    #[test]
    fn factor_file_round_trip_with_multiple_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("aapl.parquet");

        let rows = vec![
            FactorRow { date: date(2024, 1, 1), price_factor: 1.0,  split_factor: 1.0,  dividend_amount: 0.0 },
            FactorRow { date: date(2022, 8, 31), price_factor: 1.0,  split_factor: 0.25, dividend_amount: 0.0 },
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
