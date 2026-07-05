use std::collections::HashMap;

use async_trait::async_trait;
use chrono::NaiveDate;

use lean_core::{DateTime, LeanError, Resolution, Result as LeanResult, Symbol};
use lean_data::TradeBar;

use crate::client::MassiveRestClient;
use crate::corporate_actions::{
    fetch_factor_rows, fetch_map_rows, lean_factor_file_start_date, massive_aggregates_floor_date,
};
use lean_storage::{FactorFileEntry, MapFileEntry};

/// Massive historical data provider.
///
/// Massive is a **pure data source**: it fetches raw data from the Massive API
/// and returns it. All persistence is owned by the rlean framework, which writes
/// the returned rows into Iceberg (trade bars, factor files, and map files
/// alike). This provider never writes factor/map files itself.
///
/// On `get_history` (trade bars) it fetches **unadjusted** OHLCV aggregates.
/// Corporate actions are served separately via the typed `get_factor_file` /
/// `get_map_file` methods, which the framework calls and persists.
pub struct MassiveHistoryProvider {
    client: MassiveRestClient,
}

impl MassiveHistoryProvider {
    pub fn new(
        api_key: impl Into<String>,
        _data_root: impl AsRef<std::path::Path>,
        requests_per_second: f64,
    ) -> Self {
        MassiveHistoryProvider {
            client: MassiveRestClient::new(api_key.into(), requests_per_second),
        }
    }

    async fn fetch_and_cache(
        &self,
        symbol: Symbol,
        resolution: Resolution,
        start: DateTime,
        end: DateTime,
    ) -> LeanResult<Vec<TradeBar>> {
        // Download unadjusted bars — factor file handles price adjustments.
        let bars = self
            .client
            .get_aggregates(&symbol, resolution, start, end, false)
            .await
            .map_err(|e| LeanError::DataError(e.to_string()))?;

        Ok(bars)
    }

    /// Compute factor rows for `symbol`, fetching daily reference prices from
    /// Massive so dividend price factors can be computed. Returns the rows for
    /// the framework to persist; performs no writes itself.
    async fn compute_factor_rows_for(&self, symbol: &Symbol) -> anyhow::Result<Vec<FactorFileEntry>> {
        let ticker = symbol.permtick.to_uppercase();
        let action_start_day = lean_factor_file_start_date();
        let action_end_day = chrono::Utc::now().date_naive();
        let action_end = date_to_datetime(action_end_day, 23, 59, 59);

        // Fetch daily bars only for reference prices (needed to compute dividend
        // adjustment factors). A bars-endpoint permission failure must not
        // prevent split-factor generation, so treat it as empty ref prices.
        //
        // The bars API rejects pre-epoch `from` values, so we must clamp the
        // fetch to Massive's equities inception rather than passing the factor
        // file's 1900 base date (which returns a silent `status:"ERROR"` with no
        // rows and zeroes out every dividend price factor).
        let reference_start_day = action_start_day.max(massive_aggregates_floor_date());
        let mut ref_prices: HashMap<NaiveDate, f64> = HashMap::new();
        match self
            .client
            .get_aggregates(
                symbol,
                Resolution::Daily,
                date_to_datetime(reference_start_day, 0, 0, 0),
                action_end,
                false,
            )
            .await
        {
            Ok(reference_bars) => {
                for bar in reference_bars {
                    ref_prices
                        .entry(bar.time.date_utc())
                        .or_insert_with(|| bar.close.to_string().parse::<f64>().unwrap_or(0.0));
                }
                if ref_prices.is_empty() {
                    tracing::warn!(
                        "Massive: reference-price fetch for {} returned no daily bars ({} → {}); dividend price factors cannot be computed",
                        symbol.value,
                        reference_start_day,
                        action_end_day,
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    "Massive: could not fetch factor reference prices for {} ({}); dividends without reference closes will be skipped",
                    symbol.value,
                    e
                );
            }
        }

        fetch_factor_rows(
            &self.client,
            &ticker,
            action_start_day,
            action_end_day,
            &ref_prices,
        )
        .await
    }

}

fn date_to_datetime(date: NaiveDate, hour: u32, minute: u32, second: u32) -> DateTime {
    date.and_hms_opt(hour, minute, second).unwrap().into()
}

// ─── lean_data_providers::IHistoryProvider ────────────────────────────────────

#[async_trait]
impl lean_data_providers::IHistoryProvider for MassiveHistoryProvider {
    async fn get_history(
        &self,
        request: &lean_data_providers::HistoryRequest,
    ) -> anyhow::Result<Vec<TradeBar>> {
        use lean_data_providers::DataType;

        match request.data_type {
            // Corporate-action files are served via the typed `get_factor_file` /
            // `get_map_file` methods below; the framework owns persistence.
            DataType::FactorFile | DataType::MapFile => Err(anyhow::anyhow!(
                "NotImplemented: Massive serves {:?} via get_factor_file/get_map_file",
                request.data_type
            )),
            _ => self
                .fetch_and_cache(
                    request.symbol.clone(),
                    request.resolution,
                    request.start,
                    request.end,
                )
                .await
                .map_err(|e| anyhow::anyhow!("{e}")),
        }
    }

    async fn get_factor_file(&self, symbol: &Symbol) -> anyhow::Result<Vec<FactorFileEntry>> {
        self.compute_factor_rows_for(symbol).await
    }

    async fn get_map_file(&self, symbol: &Symbol) -> anyhow::Result<Vec<MapFileEntry>> {
        let ticker = symbol.permtick.to_uppercase();
        let today = chrono::Utc::now().date_naive();
        fetch_map_rows(&self.client, &ticker, today)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
    }
}
