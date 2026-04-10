/// ThetaData historical data provider — implements `IHistoricalDataProvider`.
///
/// Fetches stock EOD bars from ThetaData, writes them to the local Parquet
/// store, and returns the raw bars.  The runner applies factor-file adjustments
/// afterwards (same as the Polygon provider).
use std::future::Future;
use std::path::Path;
use std::pin::Pin;

use anyhow::Result;
use chrono::{NaiveDate, TimeZone, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use tracing::info;

use lean_core::{DateTime, LeanError, NanosecondTimestamp, Resolution, Result as LeanResult, Symbol, TimeSpan};
use lean_data::{IHistoricalDataProvider, TradeBar};
use lean_storage::{ParquetWriter, PathResolver, WriterConfig};

use crate::client::ThetaDataClient;

pub struct ThetaDataHistoryProvider {
    client:   ThetaDataClient,
    resolver: PathResolver,
    writer:   ParquetWriter,
}

impl ThetaDataHistoryProvider {
    /// Create a new provider.
    ///
    /// - `access_token`: Optional bearer token.  Not needed for a local sidecar.
    /// - `base_url`: Override the sidecar URL.  `None` → `THETADATA_BASE_URL` env
    ///   var → `http://127.0.0.1:25510`.
    pub fn new(
        access_token: Option<String>,
        base_url: Option<String>,
        data_root: impl AsRef<Path>,
        requests_per_second: f64,
        max_concurrent: usize,
    ) -> Self {
        ThetaDataHistoryProvider {
            client: ThetaDataClient::new(
                access_token,
                base_url,
                requests_per_second,
                max_concurrent,
                data_root.as_ref(),
            ),
            resolver: PathResolver::new(data_root),
            writer:   ParquetWriter::new(WriterConfig::default()),
        }
    }

    async fn fetch_and_cache(
        &self,
        symbol: Symbol,
        resolution: Resolution,
        start: DateTime,
        end: DateTime,
    ) -> LeanResult<Vec<TradeBar>> {
        let start_date = start.to_naive_utc().date();
        let end_date   = end.to_naive_utc().date();
        let ticker     = symbol.permtick.to_uppercase();

        info!("ThetaData: fetching {} {} bars for {} ({start_date} → {end_date})",
            resolution, ticker, symbol.value);

        let bars: Vec<TradeBar> = match resolution {
            Resolution::Daily => {
                let eod_bars = self.client
                    .get_stock_eod(&ticker, start_date, end_date)
                    .await
                    .map_err(|e| LeanError::DataError(e.to_string()))?;

                eod_bars.into_iter().filter_map(|b| {
                    let period = TimeSpan::ONE_DAY;
                    let time = date_to_lean_datetime(b.date, 16, 0, 0);
                    let dec = |f: f64| Decimal::from_f64(f).unwrap_or_default();
                    Some(TradeBar {
                        symbol:   symbol.clone(),
                        time,
                        end_time: NanosecondTimestamp(time.0 + period.nanos),
                        open:     dec(b.open),
                        high:     dec(b.high),
                        low:      dec(b.low),
                        close:    dec(b.close),
                        volume:   dec(b.volume),
                        period,
                    })
                }).collect()
            }
            Resolution::Minute => {
                let ohlc_bars = self.client
                    .get_stock_ohlc(&ticker, start_date, end_date, "1m", None, None)
                    .await
                    .map_err(|e| LeanError::DataError(e.to_string()))?;

                let period = TimeSpan::from_nanos(60_000_000_000);
                ohlc_bars.into_iter().filter_map(|b| {
                    let time = date_ms_to_lean_datetime(b.date, b.ms_of_day);
                    let dec  = |f: f64| Decimal::from_f64(f).unwrap_or_default();
                    Some(TradeBar {
                        symbol:   symbol.clone(),
                        time,
                        end_time: NanosecondTimestamp(time.0 + period.nanos),
                        open:     dec(b.open),
                        high:     dec(b.high),
                        low:      dec(b.low),
                        close:    dec(b.close),
                        volume:   dec(b.volume),
                        period,
                    })
                }).collect()
            }
            Resolution::Hour => {
                let ohlc_bars = self.client
                    .get_stock_ohlc(&ticker, start_date, end_date, "1h", None, None)
                    .await
                    .map_err(|e| LeanError::DataError(e.to_string()))?;

                let period = TimeSpan::from_nanos(3_600_000_000_000);
                ohlc_bars.into_iter().filter_map(|b| {
                    let time = date_ms_to_lean_datetime(b.date, b.ms_of_day);
                    let dec  = |f: f64| Decimal::from_f64(f).unwrap_or_default();
                    Some(TradeBar {
                        symbol:   symbol.clone(),
                        time,
                        end_time: NanosecondTimestamp(time.0 + period.nanos),
                        open:     dec(b.open),
                        high:     dec(b.high),
                        low:      dec(b.low),
                        close:    dec(b.close),
                        volume:   dec(b.volume),
                        period,
                    })
                }).collect()
            }
            Resolution::Second => {
                let ohlc_bars = self.client
                    .get_stock_ohlc(&ticker, start_date, end_date, "1s", None, None)
                    .await
                    .map_err(|e| LeanError::DataError(e.to_string()))?;

                let period = TimeSpan::from_nanos(1_000_000_000);
                ohlc_bars.into_iter().filter_map(|b| {
                    let time = date_ms_to_lean_datetime(b.date, b.ms_of_day);
                    let dec  = |f: f64| Decimal::from_f64(f).unwrap_or_default();
                    Some(TradeBar {
                        symbol:   symbol.clone(),
                        time,
                        end_time: NanosecondTimestamp(time.0 + period.nanos),
                        open:     dec(b.open),
                        high:     dec(b.high),
                        low:      dec(b.low),
                        close:    dec(b.close),
                        volume:   dec(b.volume),
                        period,
                    })
                }).collect()
            }
            Resolution::Tick => {
                return Err(LeanError::DataError(
                    "ThetaData: tick resolution not supported via get_trade_bars — use get_stock_trades directly".into()
                ));
            }
        };

        if bars.is_empty() {
            info!("ThetaData: no bars returned for {} [{start_date}–{end_date}]", ticker);
            return Ok(bars);
        }

        // Write to disk.
        if let Err(e) = self.write_to_disk(&symbol, resolution, &bars) {
            tracing::warn!("ThetaData: disk write failed for {}: {e}", symbol.value);
        }

        info!("ThetaData: cached {} bars for {}", bars.len(), ticker);
        Ok(bars)
    }

    fn write_to_disk(
        &self,
        symbol: &Symbol,
        resolution: Resolution,
        bars: &[TradeBar],
    ) -> Result<()> {
        use std::collections::HashMap;

        if bars.is_empty() {
            return Ok(());
        }

        // For non-date-partitioned resolutions (daily, hour) all bars share the
        // same file path regardless of date — write once.
        if !resolution.is_high_resolution() {
            let first_date = bars[0].time.to_naive_utc().date();
            let path = self.resolver.trade_bar(symbol, resolution, first_date).to_path();
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            return self.writer.write_trade_bars(bars, &path).map_err(Into::into);
        }

        // For intraday resolutions, group bars by date → one parquet file per day.
        let mut by_date: HashMap<NaiveDate, Vec<&TradeBar>> = HashMap::new();
        for bar in bars {
            let date = bar.time.to_naive_utc().date();
            by_date.entry(date).or_default().push(bar);
        }

        for (date, day_bars) in by_date {
            let owned: Vec<TradeBar> = day_bars.into_iter().cloned().collect();
            let path = self.resolver.trade_bar(symbol, resolution, date).to_path();
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            self.writer.write_trade_bars(&owned, &path)?;
        }
        Ok(())
    }
}

impl IHistoricalDataProvider for ThetaDataHistoryProvider {
    fn get_trade_bars(
        &self,
        symbol: Symbol,
        resolution: Resolution,
        start: DateTime,
        end: DateTime,
    ) -> Pin<Box<dyn Future<Output = LeanResult<Vec<TradeBar>>> + Send + '_>> {
        Box::pin(self.fetch_and_cache(symbol, resolution, start, end))
    }
}

// ─── lean_data_providers::IHistoryProvider ────────────────────────────────────
//
// IHistoryProvider::get_history is synchronous: this cdylib has its own copy
// of tokio and cannot share thread-locals with the host binary's runtime.
// We create a lightweight current-thread runtime per call; the host bridges
// to async via spawn_blocking so no tokio worker thread is blocked.

impl lean_data_providers::IHistoryProvider for ThetaDataHistoryProvider {
    fn get_history(
        &self,
        request: &lean_data_providers::HistoryRequest,
    ) -> anyhow::Result<Vec<TradeBar>> {
        use lean_data_providers::DataType;

        if request.data_type != DataType::TradeBar {
            return Err(anyhow::anyhow!(
                "NotImplemented: ThetaData does not provide {:?} data \
                 (add a provider that does, e.g. thetadata,massive)",
                request.data_type
            ));
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| anyhow::anyhow!("failed to build thetadata runtime: {e}"))?;

        rt.block_on(self.fetch_and_cache(
            request.symbol.clone(),
            request.resolution,
            request.start,
            request.end,
        ))
        .map_err(|e| anyhow::anyhow!("{e}"))
    }
}

// ─── Time helpers ─────────────────────────────────────────────────────────────

fn date_to_lean_datetime(date: NaiveDate, h: u32, m: u32, s: u32) -> NanosecondTimestamp {
    let dt = Utc.from_utc_datetime(&date.and_hms_opt(h, m, s).unwrap());
    let lean_dt = DateTime::from(dt);
    NanosecondTimestamp(lean_dt.0)
}

fn date_ms_to_lean_datetime(date: NaiveDate, ms_of_day: u32) -> NanosecondTimestamp {
    // ms_of_day is milliseconds since midnight ET.  ThetaData returns Eastern time.
    // Convert to UTC by adding 4h (EDT) or 5h (EST).  We approximate with 5h.
    let ms_utc = ms_of_day as i64 + 5 * 3_600_000;
    let midnight_utc = Utc.from_utc_datetime(&date.and_hms_opt(0, 0, 0).unwrap());
    let dt_utc = midnight_utc + chrono::Duration::milliseconds(ms_utc);
    let lean_dt = DateTime::from(dt_utc);
    NanosecondTimestamp(lean_dt.0)
}

trait ToNaiveUtc {
    fn to_naive_utc(self) -> chrono::NaiveDateTime;
}

impl ToNaiveUtc for DateTime {
    fn to_naive_utc(self) -> chrono::NaiveDateTime {
        let ns = self.0;
        let secs = ns / 1_000_000_000;
        let nanos = (ns % 1_000_000_000) as u32;
        chrono::DateTime::from_timestamp(secs, nanos)
            .unwrap_or_default()
            .naive_utc()
    }
}
