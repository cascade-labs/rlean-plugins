use std::collections::HashMap;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;

use chrono::NaiveDate;
use tracing::info;

use lean_core::{DateTime, LeanError, Resolution, Result as LeanResult, Symbol};
use lean_data::{IHistoricalDataProvider, TradeBar};
use lean_storage::{ParquetWriter, PathResolver, WriterConfig};

use crate::client::MassiveRestClient;
use crate::corporate_actions::{fetch_and_write_factor_file, fetch_and_write_map_file};

/// Massive historical data provider.
///
/// On every call to `get_trade_bars` it:
/// 1. Fetches **unadjusted** OHLCV aggregates from Massive.
/// 2. Fetches splits + dividends and computes a LEAN-compatible factor file.
/// 3. Writes bars and factor file to the local data directory.
/// 4. Returns the raw bars (callers apply the factor file separately).
pub struct MassiveHistoryProvider {
    client:   MassiveRestClient,
    resolver: PathResolver,
    writer:   ParquetWriter,
}

impl MassiveHistoryProvider {
    pub fn new(
        api_key: impl Into<String>,
        data_root: impl AsRef<Path>,
        requests_per_second: f64,
    ) -> Self {
        MassiveHistoryProvider {
            client:   MassiveRestClient::new(api_key.into(), requests_per_second),
            resolver: PathResolver::new(data_root),
            writer:   ParquetWriter::new(WriterConfig::default()),
        }
    }

    /// Path to the LEAN factor file for a symbol.
    fn factor_file_path(&self, symbol: &Symbol) -> std::path::PathBuf {
        let ticker = symbol.permtick.to_lowercase();
        let market = symbol.market().as_str().to_lowercase();
        let sec    = format!("{}", symbol.security_type()).to_lowercase();
        self.resolver.data_root
            .join(&sec)
            .join(&market)
            .join("factor_files")
            .join(format!("{ticker}.parquet"))
    }

    /// Path to the LEAN map file for a symbol.
    fn map_file_path(&self, symbol: &Symbol) -> std::path::PathBuf {
        let ticker = symbol.permtick.to_lowercase();
        let market = symbol.market().as_str().to_lowercase();
        self.resolver.data_root
            .join("equity")
            .join(&market)
            .join("map_files")
            .join(format!("{ticker}.parquet"))
    }

    async fn fetch_and_cache(
        &self,
        symbol: Symbol,
        resolution: Resolution,
        start: DateTime,
        end: DateTime,
    ) -> LeanResult<Vec<TradeBar>> {
        // Download unadjusted bars — factor file handles price adjustments.
        let bars = self.client
            .get_aggregates(&symbol, resolution, start, end, false)
            .await
            .map_err(|e| LeanError::DataError(e.to_string()))?;

        if bars.is_empty() {
            return Ok(bars);
        }

        // Write bars to disk.
        self.write_to_disk(&symbol, resolution, &bars)?;

        // For equity daily bars, also fetch corporate actions and write factor file,
        // and fetch ticker details to write the map file.
        if resolution == Resolution::Daily {
            if let Err(e) = self.fetch_and_write_factor_file(&symbol, start, end, &bars).await {
                // Non-fatal: log the error but continue.
                tracing::warn!(
                    "Massive: could not generate factor file for {}: {}",
                    symbol.value, e
                );
            }
            let map_path = self.map_file_path(&symbol);
            let ticker = symbol.permtick.to_uppercase();
            let today = chrono::Utc::now().date_naive();
            if let Err(e) = fetch_and_write_map_file(&self.client, &map_path, &ticker, today).await {
                tracing::warn!(
                    "Massive: could not generate map file for {}: {}",
                    symbol.value, e
                );
            }
        }

        Ok(bars)
    }

    async fn fetch_and_write_factor_file(
        &self,
        symbol: &Symbol,
        start: DateTime,
        end: DateTime,
        bars: &[TradeBar],
    ) -> anyhow::Result<()> {
        let ticker    = symbol.permtick.to_uppercase();
        let start_day = start.date_utc();
        let end_day   = end.date_utc();

        let ref_prices: HashMap<NaiveDate, f64> = bars
            .iter()
            .map(|b| (b.time.date_utc(), b.close.to_string().parse::<f64>().unwrap_or(0.0)))
            .collect();

        let factor_path = self.factor_file_path(symbol);
        fetch_and_write_factor_file(
            &self.client, &factor_path, &ticker, start_day, end_day, &ref_prices,
        ).await?;

        Ok(())
    }

    fn write_to_disk(
        &self,
        symbol: &Symbol,
        resolution: Resolution,
        bars: &[TradeBar],
    ) -> LeanResult<()> {
        if resolution.is_high_resolution() {
            let mut by_date: HashMap<NaiveDate, Vec<TradeBar>> = HashMap::new();
            for bar in bars {
                by_date.entry(bar.time.date_utc()).or_default().push(bar.clone());
            }
            for (date, day_bars) in by_date {
                let dp = self.resolver.trade_bar(symbol, resolution, date);
                if !dp.to_path().exists() {
                    self.writer.write_trade_bars_at(&day_bars, &dp)
                        .map_err(|e| LeanError::DataError(e.to_string()))?;
                }
            }
        } else {
            let start_date = bars[0].time.date_utc();
            let dp = self.resolver.trade_bar(symbol, resolution, start_date);
            self.writer.write_trade_bars_at(bars, &dp)
                .map_err(|e| LeanError::DataError(e.to_string()))?;
            info!(
                "Massive: cached {} bars → {}",
                bars.len(),
                dp.to_path().display()
            );
        }
        Ok(())
    }
}

impl IHistoricalDataProvider for MassiveHistoryProvider {
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

impl lean_data_providers::IHistoryProvider for MassiveHistoryProvider {
    fn get_history(
        &self,
        request: &lean_data_providers::HistoryRequest,
    ) -> anyhow::Result<Vec<TradeBar>> {
        use lean_data_providers::DataType;

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| anyhow::anyhow!("failed to build massive runtime: {e}"))?;

        match request.data_type {
            DataType::FactorFile => {
                // Fetch daily bars only for reference prices (needed to compute
                // dividend adjustment factors); do NOT write bars to disk.
                rt.block_on(async {
                    let bars = self.client
                        .get_aggregates(
                            &request.symbol,
                            lean_core::Resolution::Daily,
                            request.start,
                            request.end,
                            false,
                        )
                        .await
                        .map_err(|e| anyhow::anyhow!("{e}"))?;

                    if !bars.is_empty() {
                        self.fetch_and_write_factor_file(
                            &request.symbol,
                            request.start,
                            request.end,
                            &bars,
                        )
                        .await
                        .map_err(|e| anyhow::anyhow!("{e}"))?;
                    }

                    Ok(vec![])
                })
            }
            DataType::MapFile => {
                rt.block_on(async {
                    let map_path = self.map_file_path(&request.symbol);
                    let ticker = request.symbol.permtick.to_uppercase();
                    let today = chrono::Utc::now().date_naive();
                    fetch_and_write_map_file(&self.client, &map_path, &ticker, today)
                        .await
                        .map_err(|e| anyhow::anyhow!("{e}"))?;
                    Ok(vec![])
                })
            }
            _ => rt
                .block_on(self.fetch_and_cache(
                    request.symbol.clone(),
                    request.resolution,
                    request.start,
                    request.end,
                ))
                .map_err(|e| anyhow::anyhow!("{e}")),
        }
    }
}
