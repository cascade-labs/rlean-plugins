/// ThetaData historical data provider — implements `IHistoricalDataProvider`.
///
/// Fetches stock EOD bars from ThetaData, writes them to the local Parquet
/// store, and returns the raw bars.  The runner applies factor-file adjustments
/// afterwards (same as the Polygon provider).
use std::future::Future;
use std::path::Path;
use std::pin::Pin;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{NaiveDate, TimeZone, Utc};
use chrono_tz::America::New_York;
use futures::{stream, StreamExt};
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use tracing::info;

use lean_core::{
    DateTime, LeanError, Market, NanosecondTimestamp, OptionRight, OptionStyle, Resolution,
    Result as LeanResult, Symbol, SymbolOptionsExt, TickType, TimeSpan,
};
use lean_data::{Bar as LeanBar, IHistoricalDataProvider, QuoteBar, Tick, TradeBar, TradeBarData};
use lean_storage::{OptionUniverseRow, ParquetReader, ParquetWriter, PathResolver, WriterConfig};

use crate::client::ThetaDataClient;
use crate::models::{
    parse_date, QuoteBar as ThetaQuoteBar, TradeTick, V3OptionContract, V3OptionOhlc,
    V3OptionQuote, V3OptionTrade,
};

pub struct ThetaDataHistoryProvider {
    client: ThetaDataClient,
    resolver: PathResolver,
    writer: ParquetWriter,
    /// Earliest date this subscription tier covers.  Requests that start before
    /// this date are silently clipped to avoid HTTP 403 subscription errors.
    /// Defaults to 2018-01-01 (ThetaData STANDARD tier lower bound).
    standard_start_date: NaiveDate,
}

impl ThetaDataHistoryProvider {
    /// Create a new provider.
    ///
    /// - `access_token`: Optional bearer token.  Not needed for a local sidecar.
    /// - `base_url`: Override the sidecar URL.  `None` → `THETADATA_BASE_URL` env
    ///   var → `http://127.0.0.1:25510`.
    /// - `standard_start_date`: Earliest date supported by the subscription.
    ///   Requests before this date are clipped.  `None` → 2018-01-01.
    pub fn new(
        access_token: Option<String>,
        base_url: Option<String>,
        data_root: impl AsRef<Path>,
        requests_per_second: f64,
        max_concurrent: usize,
        standard_start_date: Option<NaiveDate>,
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
            writer: ParquetWriter::new(WriterConfig::default()),
            standard_start_date: standard_start_date
                .unwrap_or_else(|| NaiveDate::from_ymd_opt(2018, 1, 1).unwrap()),
        }
    }

    fn batch_concurrency(&self) -> usize {
        self.client.max_concurrent()
    }

    async fn fetch_trade_bars(
        &self,
        symbol: Symbol,
        resolution: Resolution,
        start: DateTime,
        end: DateTime,
    ) -> LeanResult<Vec<TradeBar>> {
        let requested_start = start.to_naive_utc().date();
        let end_date = end.to_naive_utc().date();
        let ticker = symbol.permtick.to_uppercase();

        // Clip to the subscription's lower bound to avoid HTTP 403 errors.
        let start_date = if requested_start < self.standard_start_date {
            tracing::warn!(
                "ThetaData: requested start {} is before subscription start {}; \
                 clipping to {}",
                requested_start,
                self.standard_start_date,
                self.standard_start_date
            );
            self.standard_start_date
        } else {
            requested_start
        };

        info!(
            "ThetaData: fetching {} {} bars for {} ({start_date} → {end_date})",
            resolution, ticker, symbol.value
        );

        let bars: Vec<TradeBar> = match resolution {
            Resolution::Daily => {
                let eod_bars = self
                    .client
                    .get_stock_eod(&ticker, start_date, end_date)
                    .await
                    .map_err(|e| LeanError::DataError(e.to_string()))?;

                eod_bars
                    .into_iter()
                    .map(|b| {
                        let period = TimeSpan::ONE_DAY;
                        let time = date_to_lean_datetime(b.date, 16, 0, 0);
                        let dec = |f: f64| Decimal::from_f64(f).unwrap_or_default();
                        TradeBar {
                            symbol: symbol.clone(),
                            time,
                            end_time: NanosecondTimestamp(time.0 + period.nanos),
                            open: dec(b.open),
                            high: dec(b.high),
                            low: dec(b.low),
                            close: dec(b.close),
                            volume: dec(b.volume),
                            period,
                        }
                    })
                    .collect()
            }
            Resolution::Minute => {
                let ohlc_bars = self
                    .client
                    .get_stock_ohlc(&ticker, start_date, end_date, "1m", None, None)
                    .await
                    .map_err(|e| LeanError::DataError(e.to_string()))?;

                let period = TimeSpan::from_nanos(60_000_000_000);
                ohlc_bars
                    .into_iter()
                    .map(|b| {
                        let time = date_ms_to_lean_datetime(b.date, b.ms_of_day);
                        let dec = |f: f64| Decimal::from_f64(f).unwrap_or_default();
                        TradeBar {
                            symbol: symbol.clone(),
                            time,
                            end_time: NanosecondTimestamp(time.0 + period.nanos),
                            open: dec(b.open),
                            high: dec(b.high),
                            low: dec(b.low),
                            close: dec(b.close),
                            volume: dec(b.volume),
                            period,
                        }
                    })
                    .collect()
            }
            Resolution::Hour => {
                let ohlc_bars = self
                    .client
                    .get_stock_ohlc(&ticker, start_date, end_date, "1h", None, None)
                    .await
                    .map_err(|e| LeanError::DataError(e.to_string()))?;

                let period = TimeSpan::from_nanos(3_600_000_000_000);
                ohlc_bars
                    .into_iter()
                    .map(|b| {
                        let time = date_ms_to_lean_datetime(b.date, b.ms_of_day);
                        let dec = |f: f64| Decimal::from_f64(f).unwrap_or_default();
                        TradeBar {
                            symbol: symbol.clone(),
                            time,
                            end_time: NanosecondTimestamp(time.0 + period.nanos),
                            open: dec(b.open),
                            high: dec(b.high),
                            low: dec(b.low),
                            close: dec(b.close),
                            volume: dec(b.volume),
                            period,
                        }
                    })
                    .collect()
            }
            Resolution::Second => {
                let ohlc_bars = self
                    .client
                    .get_stock_ohlc(&ticker, start_date, end_date, "1s", None, None)
                    .await
                    .map_err(|e| LeanError::DataError(e.to_string()))?;

                let period = TimeSpan::from_nanos(1_000_000_000);
                ohlc_bars
                    .into_iter()
                    .map(|b| {
                        let time = date_ms_to_lean_datetime(b.date, b.ms_of_day);
                        let dec = |f: f64| Decimal::from_f64(f).unwrap_or_default();
                        TradeBar {
                            symbol: symbol.clone(),
                            time,
                            end_time: NanosecondTimestamp(time.0 + period.nanos),
                            open: dec(b.open),
                            high: dec(b.high),
                            low: dec(b.low),
                            close: dec(b.close),
                            volume: dec(b.volume),
                            period,
                        }
                    })
                    .collect()
            }
            Resolution::Tick => {
                return Err(LeanError::DataError(
                    "ThetaData: tick resolution not supported via get_trade_bars — use get_stock_trades directly".into()
                ));
            }
        };

        if bars.is_empty() {
            info!(
                "ThetaData: no bars returned for {} [{start_date}–{end_date}]",
                ticker
            );
            return Ok(bars);
        }

        Ok(bars)
    }

    async fn fetch_and_cache(
        &self,
        symbol: Symbol,
        resolution: Resolution,
        start: DateTime,
        end: DateTime,
    ) -> LeanResult<Vec<TradeBar>> {
        let bars = self
            .fetch_trade_bars(symbol.clone(), resolution, start, end)
            .await?;

        if bars.is_empty() {
            return Ok(bars);
        }

        if let Err(e) = self.write_to_disk(&symbol, resolution, &bars) {
            tracing::warn!("ThetaData: disk write failed for {}: {e}", symbol.value);
        }

        info!(
            "ThetaData: cached {} bars for {}",
            bars.len(),
            symbol.permtick
        );
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

        // All resolutions use date partitions with all symbols for the day.
        let mut by_date: HashMap<NaiveDate, Vec<&TradeBar>> = HashMap::new();
        for bar in bars {
            let date = bar.time.to_naive_utc().date();
            by_date.entry(date).or_default().push(bar);
        }

        for (date, day_bars) in by_date {
            let owned: Vec<TradeBar> = day_bars.into_iter().cloned().collect();
            let path =
                self.resolver
                    .market_data_partition(symbol, resolution, TickType::Trade, date);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            self.writer.merge_trade_bar_partition(&owned, &path)?;
        }
        Ok(())
    }

    fn write_quote_bars_to_disk(
        &self,
        symbol: &Symbol,
        resolution: Resolution,
        bars: &[QuoteBar],
    ) -> Result<()> {
        use std::collections::HashMap;

        if bars.is_empty() {
            return Ok(());
        }

        let mut by_date: HashMap<NaiveDate, Vec<&QuoteBar>> = HashMap::new();
        for bar in bars {
            by_date
                .entry(bar.time.to_naive_utc().date())
                .or_default()
                .push(bar);
        }

        for (date, day_bars) in by_date {
            let owned: Vec<QuoteBar> = day_bars.into_iter().cloned().collect();
            let path =
                self.resolver
                    .market_data_partition(symbol, resolution, TickType::Quote, date);
            self.writer.merge_quote_bar_partition(&owned, &path)?;
        }
        Ok(())
    }

    async fn fetch_quote_bars(
        &self,
        symbol: Symbol,
        resolution: Resolution,
        start: DateTime,
        end: DateTime,
    ) -> anyhow::Result<Vec<QuoteBar>> {
        let interval = resolution_to_interval(resolution).ok_or_else(|| {
            anyhow::anyhow!("ThetaData quote bars require minute/second/hour resolution")
        })?;
        let period = resolution_to_period(resolution)
            .ok_or_else(|| anyhow::anyhow!("ThetaData quote bars require bar resolution"))?;
        let ticker = symbol.permtick.to_uppercase();
        let start_date = start.to_naive_utc().date().max(self.standard_start_date);
        let end_date = end.to_naive_utc().date();

        let rows = self
            .client
            .get_stock_quotes(&ticker, start_date, end_date, interval, None, None)
            .await?;

        Ok(rows
            .into_iter()
            .filter_map(|row| stock_quote_to_lean_quote_bar(symbol.clone(), row, period))
            .collect())
    }

    fn write_ticks_to_disk(&self, symbol: &Symbol, ticks: &[Tick]) -> Result<()> {
        use std::collections::HashMap;

        if ticks.is_empty() {
            return Ok(());
        }

        let mut by_date: HashMap<NaiveDate, Vec<&Tick>> = HashMap::new();
        for tick in ticks {
            by_date
                .entry(tick.time.to_naive_utc().date())
                .or_default()
                .push(tick);
        }

        for (date, day_ticks) in by_date {
            let owned: Vec<Tick> = day_ticks.into_iter().cloned().collect();
            let tick_type = owned
                .first()
                .map(|tick| tick.tick_type)
                .unwrap_or(TickType::Trade);
            let path =
                self.resolver
                    .market_data_partition(symbol, Resolution::Tick, tick_type, date);
            self.writer.merge_tick_partition(&owned, &path)?;
        }
        Ok(())
    }

    async fn fetch_option_universe_rows(
        &self,
        ticker: &str,
        date: NaiveDate,
    ) -> anyhow::Result<Vec<OptionUniverseRow>> {
        let contracts = self
            .client
            .get_option_contracts_for_date(ticker, date)
            .await?;
        Ok(contracts
            .iter()
            .filter_map(|row| option_contract_to_universe_row(ticker, date, row))
            .collect())
    }

    async fn fetch_option_trade_bars(
        &self,
        ticker: &str,
        resolution: Resolution,
        date: NaiveDate,
    ) -> anyhow::Result<Vec<TradeBar>> {
        let interval = resolution_to_interval(resolution).ok_or_else(|| {
            anyhow::anyhow!("ThetaData option trade bars require minute/second/hour resolution")
        })?;
        let period = resolution_to_period(resolution)
            .ok_or_else(|| anyhow::anyhow!("ThetaData option trade bars require bar resolution"))?;
        let underlying = Symbol::create_equity(ticker, &Market::usa());

        let rows = self
            .client
            .get_option_ohlc_chain_for_date(ticker, date, interval)
            .await?;
        Ok(rows
            .into_iter()
            .filter_map(|row| option_ohlc_to_trade_bar(&underlying, row, period))
            .collect())
    }

    async fn fetch_option_quote_bars(
        &self,
        ticker: &str,
        resolution: Resolution,
        date: NaiveDate,
    ) -> anyhow::Result<Vec<QuoteBar>> {
        let interval = resolution_to_interval(resolution).ok_or_else(|| {
            anyhow::anyhow!("ThetaData option quote bars require minute/second/hour resolution")
        })?;
        let period = resolution_to_period(resolution)
            .ok_or_else(|| anyhow::anyhow!("ThetaData option quote bars require bar resolution"))?;
        let underlying = Symbol::create_equity(ticker, &Market::usa());

        let rows = self
            .client
            .get_option_quote_chain_for_date(ticker, date, interval)
            .await?;
        Ok(rows
            .into_iter()
            .filter_map(|row| option_quote_to_quote_bar(&underlying, row, period))
            .collect())
    }

    async fn fetch_option_ticks(&self, ticker: &str, date: NaiveDate) -> anyhow::Result<Vec<Tick>> {
        let underlying = Symbol::create_equity(ticker, &Market::usa());

        let trade_rows = self
            .client
            .get_option_trade_chain_for_date(ticker, date)
            .await?;
        let quote_rows = self
            .client
            .get_option_quote_chain_for_date(ticker, date, "tick")
            .await?;

        let mut ticks: Vec<Tick> = quote_rows
            .into_iter()
            .filter_map(|row| option_quote_to_tick(&underlying, row))
            .collect();
        ticks.extend(
            trade_rows
                .into_iter()
                .filter_map(|row| option_trade_to_tick(&underlying, row)),
        );
        ticks.sort_by_key(|tick| (tick.time.0, tick.tick_type as u8));
        Ok(ticks)
    }

    fn write_option_universe_to_disk(
        &self,
        ticker: &str,
        date: NaiveDate,
        rows: &[OptionUniverseRow],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let path = self.resolver.option_universe_partition(date);
        let mut merged = if path.exists() {
            ParquetReader::new().read_option_universe(std::slice::from_ref(&path))?
        } else {
            Vec::new()
        };
        merged.retain(|row| !row.underlying.eq_ignore_ascii_case(ticker));
        merged.extend_from_slice(rows);
        self.writer
            .write_option_universe(&merged, &path)
            .map_err(Into::into)
    }

    fn write_option_trade_bars_to_disk(
        &self,
        _ticker: &str,
        resolution: Resolution,
        date: NaiveDate,
        bars: &[TradeBar],
    ) -> Result<()> {
        if bars.is_empty() {
            return Ok(());
        }
        let path = self
            .resolver
            .option_partition(resolution, TickType::Trade, date);
        self.writer
            .merge_trade_bar_partition(bars, &path)
            .map_err(Into::into)
    }

    fn write_option_quote_bars_to_disk(
        &self,
        _ticker: &str,
        resolution: Resolution,
        date: NaiveDate,
        bars: &[QuoteBar],
    ) -> Result<()> {
        if bars.is_empty() {
            return Ok(());
        }
        let path = self
            .resolver
            .option_partition(resolution, TickType::Quote, date);
        self.writer
            .merge_quote_bar_partition(bars, &path)
            .map_err(Into::into)
    }

    fn write_option_ticks_to_disk(
        &self,
        _ticker: &str,
        date: NaiveDate,
        ticks: &[Tick],
    ) -> Result<()> {
        if ticks.is_empty() {
            return Ok(());
        }
        let tick_type = ticks
            .first()
            .map(|tick| tick.tick_type)
            .unwrap_or(TickType::Trade);
        let path = self
            .resolver
            .option_partition(Resolution::Tick, tick_type, date);
        self.writer
            .merge_tick_partition(ticks, &path)
            .map_err(Into::into)
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

#[async_trait]
impl lean_data_providers::IHistoryProvider for ThetaDataHistoryProvider {
    fn earliest_date(&self) -> Option<chrono::NaiveDate> {
        Some(self.standard_start_date)
    }

    async fn get_history(
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

        self.fetch_and_cache(
            request.symbol.clone(),
            request.resolution,
            request.start,
            request.end,
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
    }

    async fn get_quote_bars(
        &self,
        request: &lean_data_providers::HistoryRequest,
    ) -> anyhow::Result<Vec<QuoteBar>> {
        let bars = self
            .fetch_quote_bars(
                request.symbol.clone(),
                request.resolution,
                request.start,
                request.end,
            )
            .await?;
        self.write_quote_bars_to_disk(&request.symbol, request.resolution, &bars)?;
        Ok(bars)
    }

    async fn get_ticks(
        &self,
        request: &lean_data_providers::HistoryRequest,
    ) -> anyhow::Result<Vec<Tick>> {
        let ticker = request.symbol.permtick.to_uppercase();
        let start_date = request
            .start
            .to_naive_utc()
            .date()
            .max(self.standard_start_date);
        let end_date = request.end.to_naive_utc().date();

        let rows = self
            .client
            .get_stock_trades(&ticker, start_date, end_date, None, None)
            .await?;

        let ticks: Vec<Tick> = rows
            .into_iter()
            .filter_map(|row| stock_trade_to_tick(request.symbol.clone(), row))
            .collect();

        self.write_ticks_to_disk(&request.symbol, &ticks)?;
        Ok(ticks)
    }

    async fn get_history_batch(
        &self,
        request: &lean_data_providers::HistoryBatchRequest,
    ) -> anyhow::Result<lean_data_providers::MarketDataBatch> {
        use lean_data_providers::{DataType, MarketDataBatch};

        let mut batch = MarketDataBatch::default();
        match request.data_type {
            DataType::TradeBar => {
                let results = stream::iter(request.symbols.iter().cloned())
                    .map(|symbol| async move {
                        self.fetch_trade_bars(
                            symbol,
                            request.resolution,
                            request.start,
                            request.end,
                        )
                        .await
                        .map_err(|e| anyhow::anyhow!("{e}"))
                    })
                    .buffer_unordered(self.batch_concurrency())
                    .collect::<Vec<_>>()
                    .await;

                for rows in results {
                    let rows = rows?;
                    batch.trade_bars.extend(rows);
                }

                let mut by_date: std::collections::HashMap<NaiveDate, Vec<TradeBar>> =
                    std::collections::HashMap::new();
                for bar in &batch.trade_bars {
                    by_date
                        .entry(bar.time.to_naive_utc().date())
                        .or_default()
                        .push(bar.clone());
                }
                for (date, rows) in by_date {
                    let Some(symbol) = rows.first().map(|bar| &bar.symbol) else {
                        continue;
                    };
                    let path = self.resolver.market_data_partition(
                        symbol,
                        request.resolution,
                        TickType::Trade,
                        date,
                    );
                    self.writer.merge_trade_bar_partition(&rows, &path)?;
                }
            }
            DataType::QuoteBar => {
                let results = stream::iter(request.symbols.iter().cloned())
                    .map(|symbol| async move {
                        self.fetch_quote_bars(
                            symbol,
                            request.resolution,
                            request.start,
                            request.end,
                        )
                        .await
                    })
                    .buffer_unordered(self.batch_concurrency())
                    .collect::<Vec<_>>()
                    .await;

                for rows in results {
                    let rows = rows?;
                    batch.quote_bars.extend(rows);
                }

                let mut by_date: std::collections::HashMap<NaiveDate, Vec<QuoteBar>> =
                    std::collections::HashMap::new();
                for bar in &batch.quote_bars {
                    by_date
                        .entry(bar.time.to_naive_utc().date())
                        .or_default()
                        .push(bar.clone());
                }
                for (date, rows) in by_date {
                    let Some(symbol) = rows.first().map(|bar| &bar.symbol) else {
                        continue;
                    };
                    let path = self.resolver.market_data_partition(
                        symbol,
                        request.resolution,
                        TickType::Quote,
                        date,
                    );
                    self.writer.merge_quote_bar_partition(&rows, &path)?;
                }
            }
            DataType::Tick => {
                let results = stream::iter(request.symbols.iter().cloned())
                    .map(|symbol| async move {
                        let ticker = symbol.permtick.to_uppercase();
                        let start_date = request
                            .start
                            .to_naive_utc()
                            .date()
                            .max(self.standard_start_date);
                        let end_date = request.end.to_naive_utc().date();
                        let rows = self
                            .client
                            .get_stock_trades(&ticker, start_date, end_date, None, None)
                            .await?;
                        Ok::<_, anyhow::Error>(
                            rows.into_iter()
                                .filter_map(|row| stock_trade_to_tick(symbol.clone(), row))
                                .collect::<Vec<_>>(),
                        )
                    })
                    .buffer_unordered(self.batch_concurrency())
                    .collect::<Vec<_>>()
                    .await;

                for rows in results {
                    let rows = rows?;
                    batch.ticks.extend(rows);
                }

                let mut by_date_type: std::collections::HashMap<(NaiveDate, TickType), Vec<Tick>> =
                    std::collections::HashMap::new();
                for tick in &batch.ticks {
                    by_date_type
                        .entry((tick.time.to_naive_utc().date(), tick.tick_type))
                        .or_default()
                        .push(tick.clone());
                }
                for ((date, tick_type), rows) in by_date_type {
                    let Some(symbol) = rows.first().map(|tick| &tick.symbol) else {
                        continue;
                    };
                    let path = self.resolver.market_data_partition(
                        symbol,
                        Resolution::Tick,
                        tick_type,
                        date,
                    );
                    self.writer.merge_tick_partition(&rows, &path)?;
                }
            }
            DataType::OpenInterest | DataType::FactorFile | DataType::MapFile => {
                return Err(anyhow::anyhow!(
                    "NotImplemented: ThetaData does not provide batched {:?} data",
                    request.data_type
                ));
            }
        }

        Ok(batch)
    }

    async fn get_option_history_batch(
        &self,
        request: &lean_data_providers::OptionHistoryBatchRequest,
    ) -> anyhow::Result<lean_data_providers::OptionMarketDataBatch> {
        use lean_data_providers::{OptionDataType, OptionMarketDataBatch};

        let mut batch = OptionMarketDataBatch::default();
        match request.data_type {
            OptionDataType::EodBar => {
                // Option EOD cache writes use an all-underlying daily partition.
                // Keep this path sequential until the client exposes a no-write
                // bulk EOD fetch, so concurrent roots cannot race the partition.
                for ticker in &request.tickers {
                    batch.eod_bars.extend(
                        self.client
                            .get_option_eod_bars_for_date(ticker, request.date)
                            .await?,
                    );
                }
            }
            OptionDataType::Universe => {
                let results = stream::iter(request.tickers.iter().cloned())
                    .map(|ticker| async move {
                        self.fetch_option_universe_rows(&ticker, request.date).await
                    })
                    .buffer_unordered(self.batch_concurrency())
                    .collect::<Vec<_>>()
                    .await;

                for rows in results {
                    batch.universe.extend(rows?);
                }
                let mut by_ticker: std::collections::HashMap<String, Vec<OptionUniverseRow>> =
                    std::collections::HashMap::new();
                for row in &batch.universe {
                    by_ticker
                        .entry(row.underlying.to_ascii_uppercase())
                        .or_default()
                        .push(row.clone());
                }
                for (ticker, rows) in by_ticker {
                    self.write_option_universe_to_disk(&ticker, request.date, &rows)?;
                }
            }
            OptionDataType::TradeBar => {
                let results = stream::iter(request.tickers.iter().cloned())
                    .map(|ticker| async move {
                        let universe = self
                            .fetch_option_universe_rows(&ticker, request.date)
                            .await?;
                        let bars = self
                            .fetch_option_trade_bars(&ticker, request.resolution, request.date)
                            .await?;
                        Ok::<_, anyhow::Error>((ticker, universe, bars))
                    })
                    .buffer_unordered(self.batch_concurrency())
                    .collect::<Vec<_>>()
                    .await;

                for result in results {
                    let (ticker, universe, bars) = result?;
                    self.write_option_universe_to_disk(&ticker, request.date, &universe)?;
                    batch.universe.extend(universe);
                    batch.trade_bars.extend(bars);
                }
                self.write_option_trade_bars_to_disk(
                    "",
                    request.resolution,
                    request.date,
                    &batch.trade_bars,
                )?;
            }
            OptionDataType::QuoteBar => {
                let results = stream::iter(request.tickers.iter().cloned())
                    .map(|ticker| async move {
                        let universe = self
                            .fetch_option_universe_rows(&ticker, request.date)
                            .await?;
                        let bars = self
                            .fetch_option_quote_bars(&ticker, request.resolution, request.date)
                            .await?;
                        Ok::<_, anyhow::Error>((ticker, universe, bars))
                    })
                    .buffer_unordered(self.batch_concurrency())
                    .collect::<Vec<_>>()
                    .await;

                for result in results {
                    let (ticker, universe, bars) = result?;
                    self.write_option_universe_to_disk(&ticker, request.date, &universe)?;
                    batch.universe.extend(universe);
                    batch.quote_bars.extend(bars);
                }
                self.write_option_quote_bars_to_disk(
                    "",
                    request.resolution,
                    request.date,
                    &batch.quote_bars,
                )?;
            }
            OptionDataType::Tick => {
                let results = stream::iter(request.tickers.iter().cloned())
                    .map(|ticker| async move {
                        let universe = self
                            .fetch_option_universe_rows(&ticker, request.date)
                            .await?;
                        let ticks = self.fetch_option_ticks(&ticker, request.date).await?;
                        Ok::<_, anyhow::Error>((ticker, universe, ticks))
                    })
                    .buffer_unordered(self.batch_concurrency())
                    .collect::<Vec<_>>()
                    .await;

                for result in results {
                    let (ticker, universe, ticks) = result?;
                    self.write_option_universe_to_disk(&ticker, request.date, &universe)?;
                    batch.universe.extend(universe);
                    batch.ticks.extend(ticks);
                }
                self.write_option_ticks_to_disk("", request.date, &batch.ticks)?;
            }
        }

        Ok(batch)
    }

    async fn get_option_eod_bars(
        &self,
        ticker: &str,
        date: chrono::NaiveDate,
    ) -> anyhow::Result<Vec<lean_storage::OptionEodBar>> {
        self.client.get_option_eod_bars_for_date(ticker, date).await
    }

    async fn get_option_universe(
        &self,
        ticker: &str,
        date: chrono::NaiveDate,
    ) -> anyhow::Result<Vec<OptionUniverseRow>> {
        let rows = self.fetch_option_universe_rows(ticker, date).await?;
        self.write_option_universe_to_disk(ticker, date, &rows)?;
        Ok(rows)
    }

    async fn get_option_trade_bars(
        &self,
        ticker: &str,
        resolution: Resolution,
        date: chrono::NaiveDate,
    ) -> anyhow::Result<Vec<TradeBar>> {
        let _ = self.get_option_universe(ticker, date).await?;
        let bars = self
            .fetch_option_trade_bars(ticker, resolution, date)
            .await?;
        self.write_option_trade_bars_to_disk(ticker, resolution, date, &bars)?;
        Ok(bars)
    }

    async fn get_option_quote_bars(
        &self,
        ticker: &str,
        resolution: Resolution,
        date: chrono::NaiveDate,
    ) -> anyhow::Result<Vec<QuoteBar>> {
        let _ = self.get_option_universe(ticker, date).await?;
        let bars = self
            .fetch_option_quote_bars(ticker, resolution, date)
            .await?;
        self.write_option_quote_bars_to_disk(ticker, resolution, date, &bars)?;
        Ok(bars)
    }

    async fn get_option_ticks(
        &self,
        ticker: &str,
        date: chrono::NaiveDate,
    ) -> anyhow::Result<Vec<Tick>> {
        let _ = self.get_option_universe(ticker, date).await?;
        let ticks = self.fetch_option_ticks(ticker, date).await?;
        self.write_option_ticks_to_disk(ticker, date, &ticks)?;
        Ok(ticks)
    }
}

fn resolution_to_interval(resolution: Resolution) -> Option<&'static str> {
    match resolution {
        Resolution::Second => Some("1s"),
        Resolution::Minute => Some("1m"),
        Resolution::Hour => Some("1h"),
        _ => None,
    }
}

fn resolution_to_period(resolution: Resolution) -> Option<TimeSpan> {
    match resolution {
        Resolution::Second => Some(TimeSpan::ONE_SECOND),
        Resolution::Minute => Some(TimeSpan::ONE_MINUTE),
        Resolution::Hour => Some(TimeSpan::ONE_HOUR),
        Resolution::Daily => Some(TimeSpan::ONE_DAY),
        Resolution::Tick => None,
    }
}

fn parse_option_symbol(
    underlying: &Symbol,
    expiration: &str,
    strike: f64,
    right: &str,
) -> Option<Symbol> {
    let clean = expiration.replace('-', "");
    let expiry = NaiveDate::parse_from_str(&clean, "%Y%m%d").ok()?;
    let right = match right.to_ascii_uppercase().as_str() {
        "C" | "CALL" => OptionRight::Call,
        "P" | "PUT" => OptionRight::Put,
        _ => return None,
    };
    Some(Symbol::create_option_osi(
        underlying.clone(),
        Decimal::from_f64(strike)?,
        expiry,
        right,
        OptionStyle::American,
        &Market::usa(),
    ))
}

fn row_time(date: &str, timestamp: &str, ms_of_day: u32) -> Option<NanosecondTimestamp> {
    let date = parse_date(date, timestamp)?;
    if ms_of_day > 0 {
        return Some(date_ms_to_lean_datetime(date, ms_of_day));
    }
    for fmt in &["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"] {
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(timestamp, fmt) {
            return Some(DateTime::from(dt.and_utc()));
        }
    }
    None
}

fn option_contract_to_universe_row(
    ticker: &str,
    date: NaiveDate,
    row: &V3OptionContract,
) -> Option<OptionUniverseRow> {
    let clean = row.expiration.replace('-', "");
    let expiration = NaiveDate::parse_from_str(&clean, "%Y%m%d").ok()?;
    let right = match row.right.to_ascii_uppercase().as_str() {
        "C" | "CALL" => "C",
        "P" | "PUT" => "P",
        _ => return None,
    };
    Some(OptionUniverseRow {
        date,
        symbol_value: row.symbol.clone(),
        underlying: ticker.to_uppercase(),
        expiration,
        strike: Decimal::from_f64(row.strike)?,
        right: right.to_string(),
    })
}

fn stock_quote_to_lean_quote_bar(
    symbol: Symbol,
    row: ThetaQuoteBar,
    period: TimeSpan,
) -> Option<QuoteBar> {
    let time = date_ms_to_lean_datetime(row.date, row.ms_of_day);
    let bid = Decimal::from_f64(row.bid_price)?;
    let ask = Decimal::from_f64(row.ask_price)?;
    Some(QuoteBar::new(
        symbol,
        time,
        period,
        Some(LeanBar::from_price(bid)),
        Some(LeanBar::from_price(ask)),
        Decimal::from_f64(row.bid_size).unwrap_or_default(),
        Decimal::from_f64(row.ask_size).unwrap_or_default(),
    ))
}

fn stock_trade_to_tick(symbol: Symbol, row: TradeTick) -> Option<Tick> {
    let time = date_ms_to_lean_datetime(row.date, row.ms_of_day);
    Some(Tick::trade(
        symbol,
        time,
        Decimal::from_f64(row.price)?,
        Decimal::from_f64(row.size).unwrap_or_default(),
    ))
}

fn option_ohlc_to_trade_bar(
    underlying: &Symbol,
    row: V3OptionOhlc,
    period: TimeSpan,
) -> Option<TradeBar> {
    let symbol = parse_option_symbol(underlying, &row.expiration, row.strike, &row.right)?;
    let time = row_time(&row.date, &row.timestamp, row.ms_of_day)?;
    Some(TradeBar::new(
        symbol,
        time,
        period,
        TradeBarData::new(
            Decimal::from_f64(row.open)?,
            Decimal::from_f64(row.high)?,
            Decimal::from_f64(row.low)?,
            Decimal::from_f64(row.close)?,
            Decimal::from_f64(row.volume).unwrap_or_default(),
        ),
    ))
}

fn option_quote_to_quote_bar(
    underlying: &Symbol,
    row: V3OptionQuote,
    period: TimeSpan,
) -> Option<QuoteBar> {
    let symbol = parse_option_symbol(underlying, &row.expiration, row.strike, &row.right)?;
    let time = row_time(&row.date, &row.timestamp, row.ms_of_day)?;
    let bid = Decimal::from_f64(row.bid_price)?;
    let ask = Decimal::from_f64(row.ask_price)?;
    Some(QuoteBar::new(
        symbol,
        time,
        period,
        Some(LeanBar::from_price(bid)),
        Some(LeanBar::from_price(ask)),
        Decimal::from_f64(row.bid_size).unwrap_or_default(),
        Decimal::from_f64(row.ask_size).unwrap_or_default(),
    ))
}

fn option_quote_to_tick(underlying: &Symbol, row: V3OptionQuote) -> Option<Tick> {
    let symbol = parse_option_symbol(underlying, &row.expiration, row.strike, &row.right)?;
    let time = row_time(&row.date, &row.timestamp, row.ms_of_day)?;
    Some(Tick::quote(
        symbol,
        time,
        Decimal::from_f64(row.bid_price)?,
        Decimal::from_f64(row.ask_price)?,
        Decimal::from_f64(row.bid_size).unwrap_or_default(),
        Decimal::from_f64(row.ask_size).unwrap_or_default(),
    ))
}

fn option_trade_to_tick(underlying: &Symbol, row: V3OptionTrade) -> Option<Tick> {
    let symbol = parse_option_symbol(underlying, &row.expiration, row.strike, &row.right)?;
    let time = row_time(&row.date, &row.timestamp, row.ms_of_day)?;
    Some(Tick::trade(
        symbol,
        time,
        Decimal::from_f64(row.price)?,
        Decimal::from_f64(row.size).unwrap_or_default(),
    ))
}

// ─── Time helpers ─────────────────────────────────────────────────────────────

fn date_to_lean_datetime(date: NaiveDate, h: u32, m: u32, s: u32) -> NanosecondTimestamp {
    let dt = Utc.from_utc_datetime(&date.and_hms_opt(h, m, s).unwrap());
    let lean_dt = DateTime::from(dt);
    NanosecondTimestamp(lean_dt.0)
}

fn date_ms_to_lean_datetime(date: NaiveDate, ms_of_day: u32) -> NanosecondTimestamp {
    // ms_of_day is milliseconds since midnight ET (Eastern time, DST-aware).
    // Convert to UTC using the actual New York timezone offset for this date.
    let midnight_naive = date.and_hms_opt(0, 0, 0).unwrap();
    let midnight_ny = New_York
        .from_local_datetime(&midnight_naive)
        .earliest()
        .unwrap_or_else(|| {
            // Spring-forward gap: add 1h to get past the gap, then go to midnight.
            New_York
                .from_local_datetime(&(midnight_naive + chrono::Duration::hours(1)))
                .unwrap()
        });
    let dt_utc =
        (midnight_ny + chrono::Duration::milliseconds(ms_of_day as i64)).with_timezone(&Utc);
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
