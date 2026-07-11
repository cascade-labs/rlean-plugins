/// ThetaData historical data provider — implements `lean_data_providers::IHistoryProvider`.
///
/// Fetches stock EOD bars from ThetaData, writes them to the local Parquet
/// store, and returns the raw bars.  The runner applies factor-file adjustments
/// afterwards (same as the Polygon provider).
use async_trait::async_trait;
use chrono::{NaiveDate, TimeZone, Utc};
use chrono_tz::America::New_York;
use futures::{stream, StreamExt};
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use std::collections::HashSet;
use tracing::info;

use lean_core::{
    DateTime, LeanError, Market, NanosecondTimestamp, OptionRight, OptionStyle, Resolution,
    Result as LeanResult, Symbol, SymbolOptionsExt, TimeSpan,
};
use lean_data::{Bar as LeanBar, QuoteBar, Tick, TradeBar, TradeBarData};
use lean_data_providers::TickStream;
use lean_storage::OptionUniverseRow;

use crate::client::ThetaDataClient;
use crate::models::{
    normalize_strike, parse_date, QuoteBar as ThetaQuoteBar, V3OptionContract, V3OptionOhlc,
    V3OptionQuote, V3OptionTradeQuote, V3StockTradeQuote,
};

pub struct ThetaDataHistoryProvider {
    client: ThetaDataClient,
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
        data_root: impl AsRef<std::path::Path>,
        max_concurrent: usize,
        standard_start_date: Option<NaiveDate>,
    ) -> Self {
        ThetaDataHistoryProvider {
            client: ThetaDataClient::new(
                access_token,
                base_url,
                max_concurrent,
                data_root.as_ref(),
            ),
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
            .fetch_trade_bars(symbol, resolution, start, end)
            .await?;

        info!("ThetaData: fetched {} bars", bars.len(),);
        Ok(bars)
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

    async fn fetch_option_trade_bars_for_contracts(
        &self,
        ticker: &str,
        resolution: Resolution,
        date: NaiveDate,
        contracts: &[OptionUniverseRow],
    ) -> anyhow::Result<Vec<TradeBar>> {
        if contracts.is_empty() {
            return Ok(vec![]);
        }
        let interval = resolution_to_interval(resolution).ok_or_else(|| {
            anyhow::anyhow!("ThetaData option trade bars require minute/second/hour resolution")
        })?;
        let period = resolution_to_period(resolution)
            .ok_or_else(|| anyhow::anyhow!("ThetaData option trade bars require bar resolution"))?;
        let underlying = Symbol::create_equity(ticker, &Market::usa());
        let allowed = allowed_option_symbol_values(&underlying, contracts);
        let request_contracts = option_request_contracts(contracts);

        info!(
            "ThetaData filtered option trade fetch {ticker} {date}: {} contracts",
            allowed.len()
        );
        let rows = self
            .client
            .get_option_ohlc_chain_for_contracts_for_date(
                ticker,
                date,
                interval,
                &request_contracts,
            )
            .await?;
        Ok(rows
            .into_iter()
            .filter_map(|row| option_ohlc_to_trade_bar(&underlying, row, period))
            .filter(|bar| allowed.contains(&*bar.symbol.value))
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

    async fn fetch_option_quote_bars_for_contracts(
        &self,
        ticker: &str,
        resolution: Resolution,
        date: NaiveDate,
        contracts: &[OptionUniverseRow],
    ) -> anyhow::Result<Vec<QuoteBar>> {
        if contracts.is_empty() {
            return Ok(vec![]);
        }
        let interval = resolution_to_interval(resolution).ok_or_else(|| {
            anyhow::anyhow!("ThetaData option quote bars require minute/second/hour resolution")
        })?;
        let period = resolution_to_period(resolution)
            .ok_or_else(|| anyhow::anyhow!("ThetaData option quote bars require bar resolution"))?;
        let underlying = Symbol::create_equity(ticker, &Market::usa());
        let allowed = allowed_option_symbol_values(&underlying, contracts);
        let request_contracts = option_request_contracts(contracts);

        info!(
            "ThetaData filtered option quote fetch {ticker} {date}: {} contracts",
            allowed.len()
        );
        let rows = self
            .client
            .get_option_quote_chain_for_contracts_for_date(
                ticker,
                date,
                interval,
                &request_contracts,
            )
            .await?;
        Ok(rows
            .into_iter()
            .filter_map(|row| option_quote_to_quote_bar(&underlying, row, period))
            .filter(|bar| allowed.contains(&*bar.symbol.value))
            .collect())
    }

    async fn fetch_option_ticks(&self, ticker: &str, date: NaiveDate) -> anyhow::Result<Vec<Tick>> {
        let underlying = Symbol::create_equity(ticker, &Market::usa());
        let contracts = self.fetch_option_universe_rows(ticker, date).await?;
        let trade_quote_rows = self
            .client
            .get_option_trade_quote_chain_for_filter_for_date(
                ticker,
                date,
                max_dte_from_contracts(date, &contracts),
                strike_range_from_contracts(&contracts),
            )
            .await?;

        let mut ticks: Vec<Tick> = trade_quote_rows
            .into_iter()
            .flat_map(|row| option_trade_quote_to_ticks(&underlying, row))
            .collect();
        ticks.sort_by_key(|tick| (tick.time.0, tick.tick_type as u8));
        Ok(ticks)
    }

    async fn fetch_option_ticks_for_contracts(
        &self,
        ticker: &str,
        date: NaiveDate,
        contracts: &[OptionUniverseRow],
    ) -> anyhow::Result<Vec<Tick>> {
        info!(
            "ThetaData filtered option tick fetch {ticker} {date}: {} contracts",
            contracts.len()
        );
        let underlying = Symbol::create_equity(ticker, &Market::usa());
        let allowed = allowed_option_symbol_values(&underlying, contracts);
        info!(
            "ThetaData filtered option tick fetch {ticker} {date}: {} allowed symbols",
            allowed.len()
        );

        let trade_quote_rows = self
            .client
            .get_option_trade_quote_chain_for_filter_for_date(
                ticker,
                date,
                max_dte_from_contracts(date, contracts),
                strike_range_from_contracts(contracts),
            )
            .await?;
        info!(
            "ThetaData filtered option tick fetch {ticker} {date}: {} raw trade_quote rows",
            trade_quote_rows.len()
        );
        let mut ticks: Vec<Tick> = trade_quote_rows
            .into_iter()
            .flat_map(|row| option_trade_quote_to_ticks(&underlying, row))
            .filter(|tick| allowed.contains(&*tick.symbol.value))
            .collect();
        info!(
            "ThetaData filtered option tick fetch {ticker} {date}: {} filtered trade_quote ticks",
            ticks.len()
        );

        ticks.sort_by_key(|tick| (tick.time.0, tick.tick_type as u8));
        Ok(ticks)
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
        self.fetch_quote_bars(
            request.symbol.clone(),
            request.resolution,
            request.start,
            request.end,
        )
        .await
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
            .get_stock_trade_quotes(&ticker, start_date, end_date, None, None)
            .await?;

        let ticks: Vec<Tick> = rows
            .into_iter()
            .flat_map(|row| stock_trade_quote_to_ticks(request.symbol.clone(), row))
            .collect();

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
            }
            DataType::Tick => {
                let mut seen_symbols = HashSet::new();
                let symbols: Vec<Symbol> = request
                    .symbols
                    .iter()
                    .filter_map(|symbol| {
                        if seen_symbols.insert(symbol.id.sid) {
                            Some(symbol.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                let results = stream::iter(symbols)
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
                            .get_stock_trade_quotes(&ticker, start_date, end_date, None, None)
                            .await?;
                        Ok::<_, anyhow::Error>(
                            rows.into_iter()
                                .flat_map(|row| stock_trade_quote_to_ticks(symbol.clone(), row))
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
            }
            DataType::OpenInterest
            | DataType::FactorFile
            | DataType::MapFile
            | DataType::MarginInterestRate
            | DataType::PerpetualContext => {
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
                    let (_ticker, universe, bars) = result?;
                    batch.universe.extend(universe);
                    batch.trade_bars.extend(bars);
                }
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
                    let (_ticker, universe, bars) = result?;
                    batch.universe.extend(universe);
                    batch.quote_bars.extend(bars);
                }
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
                    let (_ticker, universe, ticks) = result?;
                    batch.universe.extend(universe);
                    batch.ticks.extend(ticks);
                }
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
        self.fetch_option_universe_rows(ticker, date).await
    }

    async fn get_option_trade_bars(
        &self,
        ticker: &str,
        resolution: Resolution,
        date: chrono::NaiveDate,
    ) -> anyhow::Result<Vec<TradeBar>> {
        self.fetch_option_trade_bars(ticker, resolution, date).await
    }

    async fn get_option_trade_bars_filtered(
        &self,
        ticker: &str,
        resolution: Resolution,
        date: chrono::NaiveDate,
        contracts: &[OptionUniverseRow],
    ) -> anyhow::Result<Vec<TradeBar>> {
        self.fetch_option_trade_bars_for_contracts(ticker, resolution, date, contracts)
            .await
    }

    async fn get_option_quote_bars(
        &self,
        ticker: &str,
        resolution: Resolution,
        date: chrono::NaiveDate,
    ) -> anyhow::Result<Vec<QuoteBar>> {
        self.fetch_option_quote_bars(ticker, resolution, date).await
    }

    async fn get_option_quote_bars_filtered(
        &self,
        ticker: &str,
        resolution: Resolution,
        date: chrono::NaiveDate,
        contracts: &[OptionUniverseRow],
    ) -> anyhow::Result<Vec<QuoteBar>> {
        self.fetch_option_quote_bars_for_contracts(ticker, resolution, date, contracts)
            .await
    }

    async fn get_option_ticks(
        &self,
        ticker: &str,
        date: chrono::NaiveDate,
    ) -> anyhow::Result<Vec<Tick>> {
        let contracts = self.get_option_universe(ticker, date).await?;
        let ticks = self
            .fetch_option_ticks_for_contracts(ticker, date, &contracts)
            .await?;
        Ok(ticks)
    }

    async fn get_option_ticks_filtered(
        &self,
        ticker: &str,
        date: chrono::NaiveDate,
        contracts: &[OptionUniverseRow],
    ) -> anyhow::Result<Vec<Tick>> {
        self.fetch_option_ticks_for_contracts(ticker, date, contracts)
            .await
    }

    async fn stream_option_ticks_filtered(
        &self,
        ticker: &str,
        date: chrono::NaiveDate,
        contracts: &[OptionUniverseRow],
    ) -> anyhow::Result<TickStream> {
        if contracts.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }
        let ticks = self
            .fetch_option_ticks_for_contracts(ticker, date, contracts)
            .await?;
        info!(
            "ThetaData option tick stream {ticker} {date}: {} filtered ticks",
            ticks.len()
        );
        Ok(Box::new(ticks.into_iter().map(Ok)))
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

fn parse_option_symbol_vendor(
    underlying: &Symbol,
    expiration: &str,
    strike: f64,
    right: &str,
) -> Option<Symbol> {
    parse_option_symbol(underlying, expiration, normalize_strike(strike), right)
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
        strike: Decimal::from_f64(normalize_strike(row.strike))?,
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

fn stock_trade_quote_to_ticks(symbol: Symbol, row: V3StockTradeQuote) -> Vec<Tick> {
    let Some(quote_time) = row_time("", &row.quote_timestamp, 0) else {
        return Vec::new();
    };
    let Some(trade_time) = row_time("", &row.trade_timestamp, 0) else {
        return Vec::new();
    };
    let Some(bid) = Decimal::from_f64(row.bid_price) else {
        return Vec::new();
    };
    let Some(ask) = Decimal::from_f64(row.ask_price) else {
        return Vec::new();
    };
    let bid_size = Decimal::from_f64(row.bid_size).unwrap_or_default();
    let ask_size = Decimal::from_f64(row.ask_size).unwrap_or_default();

    let mut trade = Tick::trade(
        symbol.clone(),
        trade_time,
        Decimal::from_f64(row.price).unwrap_or_default(),
        Decimal::from_f64(row.size).unwrap_or_default(),
    );
    trade.bid_price = bid;
    trade.ask_price = ask;
    trade.bid_size = bid_size;
    trade.ask_size = ask_size;
    trade.exchange = Some(row.exchange.to_string());
    trade.sale_condition = Some(row.condition.to_string());

    vec![
        Tick::quote(symbol, quote_time, bid, ask, bid_size, ask_size),
        trade,
    ]
}

fn option_ohlc_to_trade_bar(
    underlying: &Symbol,
    row: V3OptionOhlc,
    period: TimeSpan,
) -> Option<TradeBar> {
    let symbol = parse_option_symbol_vendor(underlying, &row.expiration, row.strike, &row.right)?;
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
    let symbol = parse_option_symbol_vendor(underlying, &row.expiration, row.strike, &row.right)?;
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

fn option_trade_quote_to_ticks(underlying: &Symbol, row: V3OptionTradeQuote) -> Vec<Tick> {
    let Some(symbol) =
        parse_option_symbol_vendor(underlying, &row.expiration, row.strike, &row.right)
    else {
        return Vec::new();
    };
    let Some(quote_time) = row_time("", &row.quote_timestamp, 0) else {
        return Vec::new();
    };
    let Some(trade_time) = row_time("", &row.trade_timestamp, 0) else {
        return Vec::new();
    };
    let Some(bid) = Decimal::from_f64(row.bid_price) else {
        return Vec::new();
    };
    let Some(ask) = Decimal::from_f64(row.ask_price) else {
        return Vec::new();
    };
    let bid_size = Decimal::from_f64(row.bid_size).unwrap_or_default();
    let ask_size = Decimal::from_f64(row.ask_size).unwrap_or_default();

    let mut trade = Tick::trade(
        symbol.clone(),
        trade_time,
        Decimal::from_f64(row.price).unwrap_or_default(),
        Decimal::from_f64(row.size).unwrap_or_default(),
    );
    trade.bid_price = bid;
    trade.ask_price = ask;
    trade.bid_size = bid_size;
    trade.ask_size = ask_size;
    trade.exchange = Some(row.exchange.to_string());
    trade.sale_condition = Some(row.condition.to_string());

    vec![
        Tick::quote(symbol, quote_time, bid, ask, bid_size, ask_size),
        trade,
    ]
}

fn allowed_option_symbol_values(
    underlying: &Symbol,
    contracts: &[OptionUniverseRow],
) -> std::collections::HashSet<String> {
    contracts
        .iter()
        .filter_map(|row| {
            option_symbol_from_universe_row(underlying, row).map(|symbol| symbol.value.to_string())
        })
        .collect()
}

fn option_request_contracts(contracts: &[OptionUniverseRow]) -> Vec<(String, String)> {
    let mut out = std::collections::BTreeSet::new();
    for row in contracts {
        out.insert((
            row.expiration.format("%Y%m%d").to_string(),
            row.strike.normalize().to_string(),
        ));
    }
    out.into_iter().collect()
}

fn option_symbol_from_universe_row(underlying: &Symbol, row: &OptionUniverseRow) -> Option<Symbol> {
    let right = match row.right.to_ascii_uppercase().as_str() {
        "C" | "CALL" => OptionRight::Call,
        "P" | "PUT" => OptionRight::Put,
        _ => return None,
    };
    Some(Symbol::create_option_osi(
        underlying.clone(),
        row.strike,
        row.expiration,
        right,
        OptionStyle::American,
        &Market::usa(),
    ))
}

fn max_dte_from_contracts(date: NaiveDate, contracts: &[OptionUniverseRow]) -> i32 {
    contracts
        .iter()
        .map(|row| row.expiration.signed_duration_since(date).num_days() as i32)
        .filter(|dte| *dte >= 0)
        .max()
        .unwrap_or(0)
}

fn strike_range_from_contracts(contracts: &[OptionUniverseRow]) -> i32 {
    let mut max_count = 0usize;
    let mut by_expiry: std::collections::HashMap<NaiveDate, std::collections::BTreeSet<Decimal>> =
        std::collections::HashMap::new();
    for row in contracts {
        by_expiry
            .entry(row.expiration)
            .or_default()
            .insert(row.strike);
    }
    for strikes in by_expiry.values() {
        max_count = max_count.max(strikes.len());
    }
    ((max_count as i32) / 2).max(1)
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
