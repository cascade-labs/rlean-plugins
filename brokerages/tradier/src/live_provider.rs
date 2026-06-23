use std::collections::{BTreeSet, HashMap};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use lean_core::{
    DateTime, LeanError, NanosecondTimestamp, Resolution, Result as LeanResult, SecurityType,
    Symbol, SymbolOptionsExt, TickType, TimeSpan,
};
use lean_data::{
    live_data_channel, Bar, DataQueueHandler, LiveDataItem, LiveDataSubscription,
    LiveDataSubscriptionConfig, LiveNodePacket, QuoteBar, SubscriptionDataConfig, Tick, TradeBar,
    TradeBarData,
};
use lean_plugin::ensure_crypto_provider;
use reqwest::Client;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::json;
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};

use crate::config::{access_token_from_config, config_string, market_data_environment_from_config};
use crate::models::{TradierQuote, TradierQuoteContainer};

const DEFAULT_MARKET_WS_URL: &str = "wss://ws.tradier.com/v1/markets/events";
const MARKET_EVENT_FILTER: [&str; 4] = ["quote", "trade", "timesale", "tradex"];
const STREAM_SESSION_REFRESH_AFTER: Duration = Duration::from_secs(270);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(60);
const REST_QUOTE_POLL_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub struct TradierLiveConfig {
    pub access_token: String,
    pub use_sandbox: bool,
    pub base_url: String,
    pub valid_only: bool,
    pub linebreak: bool,
    pub reconnect_delay: Duration,
}

impl TradierLiveConfig {
    pub fn from_json(config: &serde_json::Value) -> Result<Self> {
        let access_token = access_token_from_config(config).context("missing access_token")?;

        let environment = market_data_environment_from_config(config)?;
        let custom_base_url = config_string(config, "base_url")
            .or_else(|| config_string(config, "tradier_base_url"))
            .or_else(|| config_string(config, "tradier-base-url"));
        let base_url = custom_base_url.unwrap_or_else(|| environment.base_url().to_string());
        let valid_only = config["valid_only"].as_bool().unwrap_or(true);
        let linebreak = config["linebreak"].as_bool().unwrap_or(true);
        let reconnect_delay = config["reconnect_delay_seconds"]
            .as_u64()
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(3));

        Ok(Self {
            access_token,
            use_sandbox: environment.is_sandbox(),
            base_url,
            valid_only,
            linebreak,
            reconnect_delay,
        })
    }

    fn base_url(&self) -> &str {
        &self.base_url
    }
}

pub struct TradierLiveDataProvider {
    config: TradierLiveConfig,
    state: Arc<Mutex<TradierLiveState>>,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl TradierLiveDataProvider {
    pub fn new(config: TradierLiveConfig) -> Self {
        ensure_crypto_provider();

        Self {
            config,
            state: Arc::new(Mutex::new(TradierLiveState::default())),
            stop: Arc::new(AtomicBool::new(false)),
            worker: None,
        }
    }

    fn ensure_worker(&mut self) {
        if self.worker.is_some() {
            return;
        }

        let config = self.config.clone();
        let state = self.state.clone();
        let stop = self.stop.clone();
        self.worker = Some(
            std::thread::Builder::new()
                .name("tradier-live-market-worker".to_string())
                .spawn(move || loop {
                    ensure_crypto_provider();
                    let run_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        let runtime = tokio::runtime::Builder::new_multi_thread()
                            .worker_threads(2)
                            .enable_all()
                            .thread_name("tradier-live-worker")
                            .build();
                        match runtime {
                            Ok(runtime) => {
                                if config.use_sandbox {
                                    runtime.block_on(run_tradier_rest_quotes(
                                        config.clone(),
                                        state.clone(),
                                        stop.clone(),
                                    ))
                                } else {
                                    runtime.block_on(run_tradier_stream(
                                        config.clone(),
                                        state.clone(),
                                        stop.clone(),
                                    ))
                                }
                            }
                            Err(error) => {
                                error!("Tradier live provider failed to start: {error}");
                            }
                        }
                    }));

                    match run_result {
                        Ok(()) => break,
                        Err(payload) => {
                            set_connected(&state, false);
                            if stop.load(Ordering::Relaxed) {
                                break;
                            }
                            let message = panic_message(payload.as_ref());
                            error!("Tradier live provider worker panicked: {message}; restarting");
                            eprintln!(
                                "rlean-plugin-tradier: live worker panicked: {message}; restarting"
                            );
                            std::thread::sleep(config.reconnect_delay);
                        }
                    }
                })
                .expect("failed to spawn Tradier live worker"),
        );
    }
}

impl Drop for TradierLiveDataProvider {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl DataQueueHandler for TradierLiveDataProvider {
    fn set_job(&mut self, _job: &LiveNodePacket) -> LeanResult<()> {
        Ok(())
    }

    fn subscribe(&mut self, config: &SubscriptionDataConfig) -> LeanResult<LiveDataSubscription> {
        if !tradier_supports(config) {
            return Err(LeanError::Unsupported(format!(
                "Tradier live data supports US equities and options, not {:?} {:?}",
                config.symbol.security_type(),
                config.tick_type
            )));
        }

        let (sender, receiver) = live_data_channel();
        {
            let mut state = self.state.lock().expect("Tradier live state poisoned");
            state.subscribers.insert(
                config.unique_id(),
                TradierSubscriber {
                    config: config.clone(),
                    wire_symbol: tradier_wire_symbol(&config.symbol),
                    sender,
                },
            );
            state.revision = state.revision.wrapping_add(1);
        }
        self.ensure_worker();

        Ok(LiveDataSubscription::new(
            LiveDataSubscriptionConfig::Market(config.clone()),
            receiver,
        ))
    }

    fn unsubscribe(&mut self, config: &SubscriptionDataConfig) -> LeanResult<()> {
        let mut state = self.state.lock().expect("Tradier live state poisoned");
        if state.subscribers.remove(&config.unique_id()).is_some() {
            state.revision = state.revision.wrapping_add(1);
        }
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.state
            .lock()
            .map(|state| state.connected)
            .unwrap_or(false)
    }

    fn name(&self) -> &str {
        "Tradier"
    }
}

#[derive(Default)]
struct TradierLiveState {
    subscribers: HashMap<u64, TradierSubscriber>,
    revision: u64,
    connected: bool,
}

struct TradierSubscriber {
    config: SubscriptionDataConfig,
    wire_symbol: String,
    sender: crossbeam_channel::Sender<LeanResult<LiveDataItem>>,
}

async fn run_tradier_stream(
    config: TradierLiveConfig,
    state: Arc<Mutex<TradierLiveState>>,
    stop: Arc<AtomicBool>,
) {
    let mut session_cache = TradierSessionCache::default();
    let mut reconnect_backoff = ReconnectBackoff::new(config.reconnect_delay);

    while !stop.load(Ordering::Relaxed) {
        match run_tradier_stream_once(&config, &state, &stop, &mut session_cache).await {
            Ok(()) => {
                reconnect_backoff.reset();
            }
            Err(error) => {
                let had_established_stream = is_connected(&state);
                set_connected(&state, false);
                if !stop.load(Ordering::Relaxed) {
                    let error_text = format!("{error:#}");
                    warn!("Tradier live stream disconnected: {error_text}");
                    eprintln!("rlean-plugin-tradier: live stream disconnected: {error_text}");
                    if is_session_auth_error(&error_text) {
                        fanout_error(&state, error_text);
                        return;
                    }
                    if is_session_expired_error(&error_text) {
                        session_cache.invalidate();
                    }
                    let delay = if had_established_stream {
                        reconnect_backoff.reset();
                        config.reconnect_delay
                    } else {
                        reconnect_backoff.next_delay()
                    };
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }
    set_connected(&state, false);
}

async fn run_tradier_stream_once(
    config: &TradierLiveConfig,
    state: &Arc<Mutex<TradierLiveState>>,
    stop: &Arc<AtomicBool>,
    session_cache: &mut TradierSessionCache,
) -> Result<()> {
    wait_for_subscribers(state, stop).await;
    if stop.load(Ordering::Relaxed) {
        return Ok(());
    }

    let session = session_cache.get(config).await?;
    let ws_url = tradier_websocket_url(&session.response.stream.url);
    let (mut socket, _) = tokio_tungstenite::connect_async(&ws_url)
        .await
        .with_context(|| format!("failed to connect Tradier websocket {ws_url}"))?;
    info!("Tradier live websocket connected: {ws_url}");
    eprintln!("rlean-plugin-tradier: live websocket connected");

    let mut last_signature = StreamSignature::default();
    let mut ticker = tokio::time::interval(Duration::from_millis(500));
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if stop.load(Ordering::Relaxed) {
                    return Ok(());
                }
                let signature = stream_signature(state);
                if signature != last_signature && !signature.symbols.is_empty() {
                    if !session.is_usable() {
                        bail!("Tradier streaming session expired before subscription update");
                    }
                    let symbols = signature.symbols.clone();
                    let payload = json!({
                        "symbols": symbols,
                        "filter": MARKET_EVENT_FILTER,
                        "sessionid": session.response.stream.sessionid,
                        "linebreak": config.linebreak,
                        "validOnly": config.valid_only,
                    });
                    socket
                        .send(Message::Text(payload.to_string()))
                        .await
                        .context("failed to send Tradier subscription payload")?;
                    eprintln!(
                        "rlean-plugin-tradier: sent subscription symbols={:?}",
                        signature.symbols
                    );
                    last_signature = signature;
                }
            }
            message = socket.next() => {
                let Some(message) = message else {
                    bail!("Tradier websocket closed");
                };
                match message? {
                    Message::Text(text) => {
                        if dispatch_text_message(state, &text).established_stream {
                            set_connected(state, true);
                        }
                    }
                    Message::Binary(bytes) => {
                        if let Ok(text) = String::from_utf8(bytes) {
                            if dispatch_text_message(state, &text).established_stream {
                                set_connected(state, true);
                            }
                        }
                    }
                    Message::Ping(payload) => {
                        socket
                            .send(Message::Pong(payload))
                            .await
                            .context("failed to send Tradier websocket pong")?;
                    }
                    Message::Pong(_) => {}
                    Message::Close(frame) => bail!("Tradier websocket close: {frame:?}"),
                    Message::Frame(_) => {}
                }
            }
        }
    }
}

async fn wait_for_subscribers(state: &Arc<Mutex<TradierLiveState>>, stop: &Arc<AtomicBool>) {
    while !stop.load(Ordering::Relaxed) {
        let has_subscribers = state
            .lock()
            .map(|state| !state.subscribers.is_empty())
            .unwrap_or(false);
        if has_subscribers {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn run_tradier_rest_quotes(
    config: TradierLiveConfig,
    state: Arc<Mutex<TradierLiveState>>,
    stop: Arc<AtomicBool>,
) {
    ensure_crypto_provider();

    let http = match Client::builder().timeout(Duration::from_secs(30)).build() {
        Ok(http) => http,
        Err(error) => {
            error!("Tradier paper quote provider failed to start: {error}");
            fanout_error(
                &state,
                format!("Tradier paper quote provider failed: {error}"),
            );
            return;
        }
    };

    info!("Tradier paper quote provider polling {}", config.base_url());
    eprintln!("rlean-plugin-tradier: paper quote provider polling Tradier REST quotes");

    let mut ticker = tokio::time::interval(REST_QUOTE_POLL_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    while !stop.load(Ordering::Relaxed) {
        wait_for_subscribers(&state, &stop).await;
        if stop.load(Ordering::Relaxed) {
            break;
        }

        ticker.tick().await;
        let symbols = subscribed_symbols(&state);
        if symbols.is_empty() {
            set_connected(&state, false);
            continue;
        }

        match fetch_rest_quotes(&http, &config, &symbols).await {
            Ok(quotes) => {
                set_connected(&state, true);
                dispatch_rest_quotes(&state, quotes);
            }
            Err(error) => {
                set_connected(&state, false);
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                let error_text = format!("{error:#}");
                warn!("Tradier paper quote poll failed: {error_text}");
                if is_session_auth_error(&error_text) {
                    fanout_error(&state, error_text);
                    return;
                }
                tokio::time::sleep(config.reconnect_delay).await;
            }
        }
    }

    set_connected(&state, false);
}

fn subscribed_symbols(state: &Arc<Mutex<TradierLiveState>>) -> Vec<String> {
    let state = state.lock().expect("Tradier live state poisoned");
    state
        .subscribers
        .values()
        .map(|subscriber| subscriber.wire_symbol.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

async fn fetch_rest_quotes(
    http: &Client,
    config: &TradierLiveConfig,
    symbols: &[String],
) -> Result<Vec<TradierQuote>> {
    if symbols.is_empty() {
        return Ok(Vec::new());
    }

    let csv = symbols.join(",");
    let url = format!(
        "{}/markets/quotes?symbols={csv}&greeks=false",
        config.base_url()
    );
    let response = http
        .get(url)
        .bearer_auth(&config.access_token)
        .header("Accept", "application/json")
        .send()
        .await?;
    let status = response.status();
    if !status.is_success() {
        let detail = response.text().await.unwrap_or_default();
        let detail = detail.trim();
        let suffix = if detail.is_empty() {
            String::new()
        } else {
            format!(": {detail}")
        };
        if status == 401 {
            bail!("Tradier quote request unauthorized{suffix}");
        }
        if status == 403 {
            bail!("Tradier quote request forbidden{suffix}");
        }
        if status == 429 {
            bail!("Tradier quote request rate limited{suffix}");
        }
        bail!("Tradier quote request failed with HTTP {status}{suffix}");
    }

    let container: TradierQuoteContainer = response.json().await?;
    normalize_quote_list(container)
}

fn normalize_quote_list(container: TradierQuoteContainer) -> Result<Vec<TradierQuote>> {
    let wrapper = match container.quotes {
        None => return Ok(Vec::new()),
        Some(wrapper) => wrapper,
    };
    parse_single_or_array(wrapper.quote)
}

fn parse_single_or_array<T: DeserializeOwned>(value: Value) -> Result<Vec<T>> {
    match value {
        Value::Array(_) => Ok(serde_json::from_value(value)?),
        Value::Object(_) => Ok(vec![serde_json::from_value(value)?]),
        other => bail!("expected object or array, got {other}"),
    }
}

fn dispatch_rest_quotes(state: &Arc<Mutex<TradierLiveState>>, quotes: Vec<TradierQuote>) {
    let quote_by_symbol: HashMap<String, TradierQuote> = quotes
        .into_iter()
        .map(|quote| (quote.symbol.to_ascii_uppercase(), quote))
        .collect();

    let mut state = state.lock().expect("Tradier live state poisoned");
    state.subscribers.retain(|_, subscriber| {
        let Some(quote) = quote_by_symbol.get(&subscriber.wire_symbol) else {
            return true;
        };

        let item = match subscriber.config.tick_type {
            TickType::Quote => rest_quote_to_quote_item(quote, &subscriber.config),
            TickType::Trade => rest_quote_to_trade_item(quote, &subscriber.config),
            TickType::OpenInterest => None,
        };
        item.map(|item| subscriber.sender.send(Ok(item)).is_ok())
            .unwrap_or(true)
    });
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct StreamSignature {
    revision: u64,
    symbols: Vec<String>,
}

fn stream_signature(state: &Arc<Mutex<TradierLiveState>>) -> StreamSignature {
    let state = state.lock().expect("Tradier live state poisoned");
    let symbols: BTreeSet<_> = state
        .subscribers
        .values()
        .map(|subscriber| subscriber.wire_symbol.clone())
        .collect();
    StreamSignature {
        revision: state.revision,
        symbols: symbols.into_iter().collect(),
    }
}

fn is_connected(state: &Arc<Mutex<TradierLiveState>>) -> bool {
    state.lock().map(|state| state.connected).unwrap_or(false)
}

fn set_connected(state: &Arc<Mutex<TradierLiveState>>, connected: bool) {
    if let Ok(mut state) = state.lock() {
        state.connected = connected;
    }
}

#[derive(Debug, Clone)]
struct TradierSessionLease {
    response: TradierSessionResponse,
    acquired_at: Instant,
}

impl TradierSessionLease {
    fn is_usable(&self) -> bool {
        self.acquired_at.elapsed() < STREAM_SESSION_REFRESH_AFTER
    }
}

#[derive(Debug, Default)]
struct TradierSessionCache {
    current: Option<TradierSessionLease>,
}

impl TradierSessionCache {
    async fn get(&mut self, config: &TradierLiveConfig) -> Result<TradierSessionLease> {
        if let Some(session) = &self.current {
            if session.is_usable() {
                return Ok(session.clone());
            }
        }

        let response = create_market_session(config).await?;
        info!("Created Tradier market data stream session");
        eprintln!("rlean-plugin-tradier: created market data stream session");
        let session = TradierSessionLease {
            response,
            acquired_at: Instant::now(),
        };
        self.current = Some(session.clone());
        Ok(session)
    }

    fn invalidate(&mut self) {
        self.current = None;
    }
}

#[derive(Debug)]
struct ReconnectBackoff {
    min_delay: Duration,
    max_delay: Duration,
    next_delay: Duration,
    attempt: u32,
}

impl ReconnectBackoff {
    fn new(min_delay: Duration) -> Self {
        let min_delay = min_delay.max(Duration::from_millis(250));
        Self {
            min_delay,
            max_delay: MAX_RECONNECT_DELAY,
            next_delay: min_delay,
            attempt: 0,
        }
    }

    fn reset(&mut self) {
        self.next_delay = self.min_delay;
        self.attempt = 0;
    }

    fn next_delay(&mut self) -> Duration {
        let delay = self.next_delay + self.jitter();
        self.next_delay = (self.next_delay * 2).min(self.max_delay);
        self.attempt = self.attempt.wrapping_add(1);
        delay
    }

    fn jitter(&self) -> Duration {
        let spread = self.min_delay.as_millis().min(1_000) as u64;
        if spread == 0 {
            return Duration::ZERO;
        }
        Duration::from_millis((u64::from(self.attempt) * 137) % spread)
    }
}

async fn create_market_session(config: &TradierLiveConfig) -> Result<TradierSessionResponse> {
    ensure_crypto_provider();

    let http = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .context("failed to build Tradier HTTP client")?;
    let url = format!("{}/markets/events/session", config.base_url());
    let response = http
        .post(url)
        .bearer_auth(&config.access_token)
        .header("Accept", "application/json")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .header("Content-Length", "0")
        .body("")
        .send()
        .await?;
    let status = response.status();
    if !status.is_success() {
        let detail = response.text().await.unwrap_or_default();
        let detail = detail.trim();
        let suffix = if detail.is_empty() {
            String::new()
        } else {
            format!(": {detail}")
        };
        if status == 401 {
            bail!("Tradier streaming session unauthorized{suffix}");
        }
        if status == 403 {
            bail!("Tradier streaming session forbidden{suffix}");
        }
        bail!("Tradier streaming session failed with HTTP {status}{suffix}");
    }
    Ok(response.json::<TradierSessionResponse>().await?)
}

#[derive(Debug, Clone, Deserialize)]
struct TradierSessionResponse {
    stream: TradierSession,
}

#[derive(Debug, Clone, Deserialize)]
struct TradierSession {
    #[serde(default)]
    url: String,
    sessionid: String,
}

#[derive(Debug, Default)]
struct DispatchResult {
    established_stream: bool,
}

fn dispatch_text_message(state: &Arc<Mutex<TradierLiveState>>, text: &str) -> DispatchResult {
    let mut result = DispatchResult::default();
    for line in text.lines().map(str::trim).filter(|line| !line.is_empty()) {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(line) {
            if value["success"].as_bool() == Some(true) {
                result.established_stream = true;
                continue;
            }
            if let Some(error) = value["error"].as_str() {
                warn!("Tradier stream error: {error}");
                fanout_error(state, format!("Tradier stream error: {error}"));
                continue;
            }
        }
        match serde_json::from_str::<TradierStreamEvent>(line) {
            Ok(event) => {
                if event.symbol().is_some() {
                    result.established_stream = true;
                }
                dispatch_event(state, event);
            }
            Err(error) => debug!("ignoring Tradier websocket payload: {error}: {line}"),
        }
    }
    result
}

fn dispatch_event(state: &Arc<Mutex<TradierLiveState>>, event: TradierStreamEvent) {
    let Some(symbol) = event.symbol() else {
        return;
    };
    let mut state = state.lock().expect("Tradier live state poisoned");
    state.subscribers.retain(|_, subscriber| {
        if !subscriber.wire_symbol.eq_ignore_ascii_case(symbol) {
            return true;
        }

        let item = match subscriber.config.tick_type {
            TickType::Quote => event_to_quote_item(&event, &subscriber.config),
            TickType::Trade => event_to_trade_item(&event, &subscriber.config),
            TickType::OpenInterest => None,
        };
        item.map(|item| subscriber.sender.send(Ok(item)).is_ok())
            .unwrap_or(true)
    });
}

fn fanout_error(state: &Arc<Mutex<TradierLiveState>>, error: String) {
    let mut state = state.lock().expect("Tradier live state poisoned");
    state.subscribers.retain(|_, subscriber| {
        subscriber
            .sender
            .send(Err(LeanError::DataError(error.clone())))
            .is_ok()
    });
}

fn is_session_auth_error(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    error.contains("unauthorized") || error.contains("forbidden")
}

fn is_session_expired_error(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    error.contains("session") && (error.contains("expired") || error.contains("invalid"))
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        return (*message).to_string();
    }
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    "unknown panic payload".to_string()
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum TradierStreamEvent {
    Quote {
        symbol: String,
        bid: FlexibleDecimal,
        ask: FlexibleDecimal,
        bidsz: FlexibleDecimal,
        asksz: FlexibleDecimal,
        biddate: FlexibleI64,
        askdate: FlexibleI64,
    },
    Trade {
        symbol: String,
        #[serde(default)]
        exch: Option<String>,
        price: FlexibleDecimal,
        size: FlexibleDecimal,
        date: FlexibleI64,
    },
    Tradex {
        symbol: String,
        #[serde(default)]
        exch: Option<String>,
        price: FlexibleDecimal,
        size: FlexibleDecimal,
        date: FlexibleI64,
    },
    Timesale {
        symbol: String,
        #[serde(default)]
        exch: Option<String>,
        last: FlexibleDecimal,
        size: FlexibleDecimal,
        date: FlexibleI64,
        #[serde(default)]
        flag: Option<String>,
        #[serde(default)]
        cancel: bool,
        #[serde(default)]
        correction: bool,
    },
    Summary {
        symbol: String,
    },
    #[serde(other)]
    Other,
}

impl TradierStreamEvent {
    fn symbol(&self) -> Option<&str> {
        match self {
            Self::Quote { symbol, .. }
            | Self::Trade { symbol, .. }
            | Self::Tradex { symbol, .. }
            | Self::Timesale { symbol, .. }
            | Self::Summary { symbol } => Some(symbol),
            Self::Other => None,
        }
    }
}

fn event_to_quote_item(
    event: &TradierStreamEvent,
    config: &SubscriptionDataConfig,
) -> Option<LiveDataItem> {
    let TradierStreamEvent::Quote {
        bid,
        ask,
        bidsz,
        asksz,
        biddate,
        askdate,
        ..
    } = event
    else {
        return None;
    };

    if bid.0 <= Decimal::ZERO || ask.0 <= Decimal::ZERO {
        warn!(
            "Tradier quote event has non-positive bid/ask for {}: bid={} ask={}",
            config.symbol.value, bid.0, ask.0
        );
        return None;
    }
    let Some(time) = quote_time(*biddate, *askdate) else {
        warn!(
            "Tradier quote event has invalid timestamps for {}: biddate={} askdate={}",
            config.symbol.value, biddate.0, askdate.0
        );
        return None;
    };
    if config.resolution == Resolution::Tick {
        return Some(LiveDataItem::Tick(Tick::quote(
            config.symbol.clone(),
            time,
            bid.0,
            ask.0,
            bidsz.0,
            asksz.0,
        )));
    }

    let period = config
        .resolution
        .to_time_span()
        .unwrap_or(TimeSpan::ONE_SECOND);
    let bucket = floor_time(time, period);
    Some(LiveDataItem::QuoteBar(QuoteBar::new(
        config.symbol.clone(),
        bucket,
        period,
        positive_bar(bid.0),
        positive_bar(ask.0),
        bidsz.0,
        asksz.0,
    )))
}

fn event_to_trade_item(
    event: &TradierStreamEvent,
    config: &SubscriptionDataConfig,
) -> Option<LiveDataItem> {
    let (price, size, date, exchange, sale_condition, suspicious) = match event {
        TradierStreamEvent::Trade {
            price,
            size,
            date,
            exch,
            ..
        }
        | TradierStreamEvent::Tradex {
            price,
            size,
            date,
            exch,
            ..
        } => (price.0, size.0, *date, exch.clone(), None, false),
        TradierStreamEvent::Timesale {
            last,
            size,
            date,
            exch,
            flag,
            cancel,
            correction,
            ..
        } => (
            last.0,
            size.0,
            *date,
            exch.clone(),
            flag.clone(),
            *cancel || *correction,
        ),
        _ => return None,
    };

    if price <= Decimal::ZERO {
        warn!(
            "Tradier trade event has non-positive price for {}: price={price}",
            config.symbol.value
        );
        return None;
    }
    if size <= Decimal::ZERO {
        warn!(
            "Tradier trade event has non-positive size for {}: size={size}",
            config.symbol.value
        );
        return None;
    }

    let Some(time) = date_time(date) else {
        warn!(
            "Tradier trade event has invalid timestamp for {}: date={}",
            config.symbol.value, date.0
        );
        return None;
    };
    if config.resolution == Resolution::Tick {
        let mut tick = Tick::trade(config.symbol.clone(), time, price, size);
        tick.exchange = exchange;
        tick.sale_condition = sale_condition;
        tick.suspicious = suspicious;
        return Some(LiveDataItem::Tick(tick));
    }

    let period = config
        .resolution
        .to_time_span()
        .unwrap_or(TimeSpan::ONE_SECOND);
    let bucket = floor_time(time, period);
    Some(LiveDataItem::TradeBar(TradeBar::new(
        config.symbol.clone(),
        bucket,
        period,
        TradeBarData::new(price, price, price, price, size),
    )))
}

fn rest_quote_to_quote_item(
    quote: &TradierQuote,
    config: &SubscriptionDataConfig,
) -> Option<LiveDataItem> {
    let Some(bid) = positive_decimal_from_f64(quote.bid) else {
        warn!(
            "Tradier REST quote has non-positive bid for {}: bid={}",
            config.symbol.value, quote.bid
        );
        return None;
    };
    let Some(ask) = positive_decimal_from_f64(quote.ask) else {
        warn!(
            "Tradier REST quote has non-positive ask for {}: ask={}",
            config.symbol.value, quote.ask
        );
        return None;
    };

    let time = rest_quote_time(quote.bid_date.max(quote.ask_date));
    let bid_size = non_negative_i64_decimal(quote.bidsize);
    let ask_size = non_negative_i64_decimal(quote.asksize);

    if config.resolution == Resolution::Tick {
        return Some(LiveDataItem::Tick(Tick::quote(
            config.symbol.clone(),
            time,
            bid,
            ask,
            bid_size,
            ask_size,
        )));
    }

    let period = config
        .resolution
        .to_time_span()
        .unwrap_or(TimeSpan::ONE_SECOND);
    let bucket = floor_time(time, period);
    Some(LiveDataItem::QuoteBar(QuoteBar::new(
        config.symbol.clone(),
        bucket,
        period,
        positive_bar(bid),
        positive_bar(ask),
        bid_size,
        ask_size,
    )))
}

fn rest_quote_to_trade_item(
    quote: &TradierQuote,
    config: &SubscriptionDataConfig,
) -> Option<LiveDataItem> {
    let Some(price) = positive_decimal_from_f64(quote.last) else {
        warn!(
            "Tradier REST quote has non-positive last price for {}: last={}",
            config.symbol.value, quote.last
        );
        return None;
    };

    let time = rest_quote_time(quote.trade_date);
    let volume = positive_i64_decimal(quote.last_volume)
        .or_else(|| positive_i64_decimal(quote.volume))
        .unwrap_or(Decimal::ZERO);

    if config.resolution == Resolution::Tick {
        return Some(LiveDataItem::Tick(Tick::trade(
            config.symbol.clone(),
            time,
            price,
            volume,
        )));
    }

    let period = config
        .resolution
        .to_time_span()
        .unwrap_or(TimeSpan::ONE_SECOND);
    let bucket = floor_time(time, period);
    Some(LiveDataItem::TradeBar(TradeBar::new(
        config.symbol.clone(),
        bucket,
        period,
        TradeBarData::new(price, price, price, price, volume),
    )))
}

fn positive_bar(price: Decimal) -> Option<Bar> {
    (price > Decimal::ZERO).then(|| Bar::from_price(price))
}

fn positive_decimal_from_f64(value: f64) -> Option<Decimal> {
    if value <= 0.0 || !value.is_finite() {
        return None;
    }
    Decimal::from_f64(value)
}

fn positive_i64_decimal(value: i64) -> Option<Decimal> {
    if value <= 0 {
        return None;
    }
    Decimal::from_i64(value)
}

fn non_negative_i64_decimal(value: i64) -> Decimal {
    Decimal::from_i64(value.max(0)).unwrap_or(Decimal::ZERO)
}

fn quote_time(biddate: FlexibleI64, askdate: FlexibleI64) -> Option<DateTime> {
    let millis = biddate.0.max(askdate.0);
    if millis > 0 {
        Some(DateTime::from_millis(millis))
    } else {
        None
    }
}

fn date_time(date: FlexibleI64) -> Option<DateTime> {
    if date.0 > 0 {
        Some(DateTime::from_millis(date.0))
    } else {
        None
    }
}

fn rest_quote_time(millis: i64) -> DateTime {
    if millis > 0 {
        DateTime::from_millis(millis)
    } else {
        DateTime::now()
    }
}

fn floor_time(time: DateTime, period: TimeSpan) -> DateTime {
    NanosecondTimestamp(time.0 - time.0.rem_euclid(period.nanos))
}

fn tradier_supports(config: &SubscriptionDataConfig) -> bool {
    matches!(
        config.symbol.security_type(),
        SecurityType::Equity | SecurityType::Option | SecurityType::IndexOption
    ) && matches!(config.tick_type, TickType::Trade | TickType::Quote)
}

fn tradier_wire_symbol(symbol: &Symbol) -> String {
    if symbol.security_type().is_option_like() {
        symbol
            .option_symbol_id()
            .map(|id| {
                lean_core::format_option_ticker(
                    &id.underlying.permtick,
                    id.strike,
                    id.expiry,
                    id.right,
                )
            })
            .unwrap_or_else(|| symbol.permtick.clone())
            .to_ascii_uppercase()
    } else {
        symbol.permtick.to_ascii_uppercase()
    }
}

fn tradier_websocket_url(session_url: &str) -> String {
    let session_url = session_url.trim();
    if session_url.is_empty() {
        return DEFAULT_MARKET_WS_URL.to_string();
    }
    if let Some(rest) = session_url.strip_prefix("https://") {
        return format!("wss://{rest}");
    }
    if let Some(rest) = session_url.strip_prefix("http://") {
        return format!("ws://{rest}");
    }
    session_url.to_string()
}

#[derive(Debug, Clone, Copy)]
struct FlexibleDecimal(Decimal);

impl<'de> Deserialize<'de> for FlexibleDecimal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        let decimal = match value {
            serde_json::Value::Number(number) => number
                .as_f64()
                .and_then(Decimal::from_f64)
                .ok_or_else(|| serde::de::Error::custom("invalid Tradier decimal number"))?,
            serde_json::Value::String(value) => value
                .parse()
                .map_err(|_| serde::de::Error::custom("invalid Tradier decimal string"))?,
            serde_json::Value::Null => {
                return Err(serde::de::Error::custom("missing Tradier decimal value"));
            }
            _ => return Err(serde::de::Error::custom("invalid Tradier decimal type")),
        };
        Ok(Self(decimal))
    }
}

#[derive(Debug, Clone, Copy)]
struct FlexibleI64(i64);

impl<'de> Deserialize<'de> for FlexibleI64 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        let integer = match value {
            serde_json::Value::Number(number) => number
                .as_i64()
                .ok_or_else(|| serde::de::Error::custom("invalid Tradier integer number"))?,
            serde_json::Value::String(value) => value
                .parse()
                .map_err(|_| serde::de::Error::custom("invalid Tradier integer string"))?,
            serde_json::Value::Null => {
                return Err(serde::de::Error::custom("missing Tradier integer value"));
            }
            _ => return Err(serde::de::Error::custom("invalid Tradier integer type")),
        };
        Ok(Self(integer))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lean_core::{Market, OptionRight, OptionStyle};
    use rust_decimal_macros::dec;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{atomic::AtomicUsize, mpsc};

    #[test]
    fn live_config_allows_paper_rest_quotes() {
        let config = serde_json::json!({
            "access_token": "token",
            "tradier-environment": "paper",
        });

        let config = TradierLiveConfig::from_json(&config).unwrap();

        assert!(config.use_sandbox);
        assert_eq!(config.base_url, crate::config::SANDBOX_BASE);
    }

    #[test]
    fn formats_equity_wire_symbol() {
        let symbol = Symbol::create_equity("spy", &Market::usa());
        assert_eq!(tradier_wire_symbol(&symbol), "SPY");
    }

    #[test]
    fn formats_option_wire_symbol() {
        let underlying = Symbol::create_equity("SPY", &Market::usa());
        let option = Symbol::create_option_osi(
            underlying,
            Decimal::from_i32(450).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2025, 1, 17).unwrap(),
            OptionRight::Call,
            OptionStyle::American,
            &Market::usa(),
        );
        assert_eq!(tradier_wire_symbol(&option), "SPY250117C00450000");
    }

    #[test]
    fn formats_index_option_wire_symbol() {
        let underlying = Symbol::create_index("SPX", &Market::usa());
        let option = Symbol::create_index_option_osi(
            underlying,
            Decimal::from_i32(4500).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2025, 1, 17).unwrap(),
            OptionRight::Put,
            OptionStyle::European,
            &Market::usa(),
        );
        assert_eq!(tradier_wire_symbol(&option), "SPX250117P04500000");
    }

    #[test]
    fn parses_quote_event_to_quote_bar() {
        let symbol = Symbol::create_equity("SPY", &Market::usa());
        let mut config = SubscriptionDataConfig::new_equity(symbol, Resolution::Minute);
        config.tick_type = TickType::Quote;
        let event: TradierStreamEvent = serde_json::from_str(
            r#"{"type":"quote","symbol":"SPY","bid":281.84,"bidsz":60,"biddate":"1557757189000","ask":281.85,"asksz":6,"askdate":"1557757190000"}"#,
        )
        .unwrap();
        let item = event_to_quote_item(&event, &config).unwrap();
        match item {
            LiveDataItem::QuoteBar(bar) => {
                assert_eq!(bar.symbol.value, "SPY");
                assert_eq!(bar.last_bid_size, Decimal::from_i32(60).unwrap());
                assert_eq!(bar.last_ask_size, Decimal::from_i32(6).unwrap());
            }
            other => panic!("unexpected item: {other:?}"),
        }
    }

    #[test]
    fn parses_trade_event_to_trade_bar() {
        let symbol = Symbol::create_equity("SPY", &Market::usa());
        let config = SubscriptionDataConfig::new_equity(symbol, Resolution::Minute);
        let event: TradierStreamEvent = serde_json::from_str(
            r#"{"type":"trade","symbol":"SPY","exch":"J","price":"281.85","size":"100","date":"1557757190000","last":"281.85"}"#,
        )
        .unwrap();
        let item = event_to_trade_item(&event, &config).unwrap();
        match item {
            LiveDataItem::TradeBar(bar) => {
                assert_eq!(bar.symbol.value, "SPY");
                assert_eq!(bar.close, Decimal::from_f64(281.85).unwrap());
                assert_eq!(bar.volume, Decimal::from_i32(100).unwrap());
            }
            other => panic!("unexpected item: {other:?}"),
        }
    }

    #[test]
    fn rest_quote_to_trade_bar_uses_last_price() {
        let symbol = Symbol::create_equity("SPY", &Market::usa());
        let config = SubscriptionDataConfig::new_equity(symbol, Resolution::Minute);

        let item = rest_quote_to_trade_item(&sample_rest_quote(), &config).unwrap();

        match item {
            LiveDataItem::TradeBar(bar) => {
                assert_eq!(bar.symbol.value, "SPY");
                assert_eq!(bar.open, Decimal::from_f64(734.15).unwrap());
                assert_eq!(bar.high, Decimal::from_f64(734.15).unwrap());
                assert_eq!(bar.low, Decimal::from_f64(734.15).unwrap());
                assert_eq!(bar.close, Decimal::from_f64(734.15).unwrap());
                assert_eq!(bar.volume, Decimal::from_i64(125).unwrap());
            }
            other => panic!("unexpected item: {other:?}"),
        }
    }

    #[test]
    fn rest_quote_to_quote_bar_uses_bid_ask() {
        let symbol = Symbol::create_equity("SPY", &Market::usa());
        let mut config = SubscriptionDataConfig::new_equity(symbol, Resolution::Minute);
        config.tick_type = TickType::Quote;

        let item = rest_quote_to_quote_item(&sample_rest_quote(), &config).unwrap();

        match item {
            LiveDataItem::QuoteBar(bar) => {
                assert_eq!(bar.symbol.value, "SPY");
                assert_eq!(bar.last_bid_size, Decimal::from_i64(7).unwrap());
                assert_eq!(bar.last_ask_size, Decimal::from_i64(8).unwrap());
                assert_eq!(
                    bar.bid.as_ref().map(|bar| bar.close),
                    Some(Decimal::from_f64(734.14).unwrap())
                );
                assert_eq!(
                    bar.ask.as_ref().map(|bar| bar.close),
                    Some(Decimal::from_f64(734.16).unwrap())
                );
            }
            other => panic!("unexpected item: {other:?}"),
        }
    }

    #[test]
    fn rest_quote_to_trade_bar_rejects_zero_last_price() {
        let symbol = Symbol::create_equity("SPY", &Market::usa());
        let config = SubscriptionDataConfig::new_equity(symbol, Resolution::Minute);
        let quote = TradierQuote {
            last: 0.0,
            bid: 734.14,
            ask: 734.16,
            ..sample_rest_quote()
        };

        assert!(rest_quote_to_trade_item(&quote, &config).is_none());
    }

    #[test]
    fn live_quote_event_rejects_null_numeric_fields() {
        let error = serde_json::from_str::<TradierStreamEvent>(
            r#"{"type":"quote","symbol":"SPY","bid":null,"bidsz":60,"biddate":"1557757189000","ask":281.85,"asksz":6,"askdate":"1557757190000"}"#,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("missing Tradier decimal value"));
    }

    #[test]
    fn live_quote_event_rejects_missing_required_fields() {
        let error = serde_json::from_str::<TradierStreamEvent>(
            r#"{"type":"quote","symbol":"SPY","bid":281.84,"bidsz":60,"biddate":"1557757189000","asksz":6,"askdate":"1557757190000"}"#,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("missing field `ask`"));
    }

    #[test]
    fn live_quote_event_does_not_publish_zero_prices() {
        let symbol = Symbol::create_equity("SPY", &Market::usa());
        let mut config = SubscriptionDataConfig::new_equity(symbol, Resolution::Tick);
        config.tick_type = TickType::Quote;
        let event: TradierStreamEvent = serde_json::from_str(
            r#"{"type":"quote","symbol":"SPY","bid":"0","bidsz":"60","biddate":"1557757189000","ask":"281.85","asksz":"6","askdate":"1557757190000"}"#,
        )
        .unwrap();

        assert!(event_to_quote_item(&event, &config).is_none());
    }

    #[test]
    fn live_trade_event_rejects_invalid_numeric_strings() {
        let error = serde_json::from_str::<TradierStreamEvent>(
            r#"{"type":"trade","symbol":"SPY","exch":"J","price":"bad","size":"100","date":"1557757190000"}"#,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("invalid Tradier decimal string"));
    }

    #[test]
    fn live_trade_event_does_not_publish_zero_prices() {
        let symbol = Symbol::create_equity("SPY", &Market::usa());
        let config = SubscriptionDataConfig::new_equity(symbol, Resolution::Tick);
        let event: TradierStreamEvent = serde_json::from_str(
            r#"{"type":"trade","symbol":"SPY","exch":"J","price":"0","size":"100","date":"1557757190000"}"#,
        )
        .unwrap();

        assert!(event_to_trade_item(&event, &config).is_none());
    }

    #[test]
    fn provider_subscribes_and_receives_mock_websocket_quote() {
        let (base_url, payload_receiver) = spawn_mock_tradier_stream();
        let mut provider = TradierLiveDataProvider::new(TradierLiveConfig {
            access_token: "test-token".to_string(),
            use_sandbox: false,
            base_url,
            valid_only: true,
            linebreak: true,
            reconnect_delay: Duration::from_millis(25),
        });
        let mut config = SubscriptionDataConfig::new_equity(
            Symbol::create_equity("SPY", &Market::usa()),
            Resolution::Tick,
        );
        config.tick_type = TickType::Quote;

        let subscription = provider.subscribe(&config).unwrap();
        let item = subscription
            .receiver
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        let payload = payload_receiver
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
        let payload: serde_json::Value = serde_json::from_str(&payload).unwrap();

        assert_eq!(payload["sessionid"], "mock-session");
        assert_eq!(payload["symbols"], serde_json::json!(["SPY"]));
        assert_eq!(
            payload["filter"],
            serde_json::json!(["quote", "trade", "timesale", "tradex"])
        );
        match item {
            LiveDataItem::Tick(tick) => {
                assert_eq!(tick.symbol.value, "SPY");
                assert_eq!(tick.bid_price, dec!(450.10));
                assert_eq!(tick.ask_price, dec!(450.12));
            }
            other => panic!("unexpected item: {other:?}"),
        }
    }

    #[test]
    fn provider_reuses_market_session_across_fast_websocket_reconnect() {
        let (base_url, payload_receiver, session_requests) =
            spawn_reconnecting_mock_tradier_stream();
        let mut provider = TradierLiveDataProvider::new(TradierLiveConfig {
            access_token: "test-token".to_string(),
            use_sandbox: false,
            base_url,
            valid_only: true,
            linebreak: true,
            reconnect_delay: Duration::from_millis(25),
        });
        let mut config = SubscriptionDataConfig::new_equity(
            Symbol::create_equity("SPY", &Market::usa()),
            Resolution::Tick,
        );
        config.tick_type = TickType::Quote;

        let subscription = provider.subscribe(&config).unwrap();
        let first_payload: serde_json::Value = serde_json::from_str(
            &payload_receiver
                .recv_timeout(Duration::from_secs(5))
                .unwrap(),
        )
        .unwrap();
        let second_payload: serde_json::Value = serde_json::from_str(
            &payload_receiver
                .recv_timeout(Duration::from_secs(5))
                .unwrap(),
        )
        .unwrap();
        let item = subscription
            .receiver
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();

        assert_eq!(first_payload["sessionid"], "mock-session");
        assert_eq!(second_payload["sessionid"], "mock-session");
        assert_eq!(
            session_requests.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        match item {
            LiveDataItem::Tick(tick) => {
                assert_eq!(tick.symbol.value, "SPY");
                assert_eq!(tick.bid_price, dec!(450.10));
                assert_eq!(tick.ask_price, dec!(450.12));
            }
            other => panic!("unexpected item: {other:?}"),
        }
    }

    #[test]
    fn dispatch_success_payload_marks_stream_established() {
        let state = Arc::new(Mutex::new(TradierLiveState::default()));
        let result = dispatch_text_message(&state, r#"{"success":true}"#);
        assert!(result.established_stream);
    }

    #[test]
    fn reconnect_backoff_grows_and_resets() {
        let mut backoff = ReconnectBackoff::new(Duration::from_millis(25));
        let first = backoff.next_delay();
        let second = backoff.next_delay();
        assert!(second > first);

        backoff.reset();
        assert_eq!(backoff.next_delay(), first);
    }

    #[test]
    fn tradier_stream_session_url_is_normalized_for_websocket() {
        assert_eq!(
            tradier_websocket_url("https://stream.tradier.com/v1/markets/events"),
            "wss://stream.tradier.com/v1/markets/events"
        );
        assert_eq!(
            tradier_websocket_url("http://127.0.0.1:1234/v1/markets/events"),
            "ws://127.0.0.1:1234/v1/markets/events"
        );
        assert_eq!(
            tradier_websocket_url("wss://stream.tradier.com/v1/markets/events"),
            "wss://stream.tradier.com/v1/markets/events"
        );
        assert_eq!(tradier_websocket_url(""), DEFAULT_MARKET_WS_URL);
    }

    fn sample_rest_quote() -> TradierQuote {
        TradierQuote {
            symbol: "SPY".to_string(),
            last: 734.15,
            bid: 734.14,
            ask: 734.16,
            volume: 1_000_000,
            last_volume: 125,
            trade_date: 1_557_757_190_000,
            bid_date: 1_557_757_189_000,
            ask_date: 1_557_757_190_000,
            bidsize: 7,
            asksize: 8,
            ..Default::default()
        }
    }

    fn spawn_mock_tradier_stream() -> (String, mpsc::Receiver<String>) {
        let (payload_sender, payload_receiver) = mpsc::channel();

        let ws_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let ws_addr = ws_listener.local_addr().unwrap();
        ws_listener.set_nonblocking(true).unwrap();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let listener = tokio::net::TcpListener::from_std(ws_listener).unwrap();
                let (stream, _) = listener.accept().await.unwrap();
                let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
                if let Some(Ok(Message::Text(payload))) = socket.next().await {
                    payload_sender.send(payload).unwrap();
                }
                socket
                    .send(Message::Text(
                        r#"{"type":"quote","symbol":"SPY","bid":"450.10","ask":"450.12","bidsz":"7","asksz":"8","biddate":"1557757189000","askdate":"1557757190000"}"#
                            .to_string(),
                    ))
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(25)).await;
            });
        });

        let http_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let http_addr = http_listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let (mut stream, _) = http_listener.accept().unwrap();
            let mut request = [0_u8; 4096];
            let bytes = stream.read(&mut request).unwrap();
            let request = String::from_utf8_lossy(&request[..bytes]);
            assert!(request.starts_with("POST /markets/events/session "));
            assert!(request
                .to_ascii_lowercase()
                .contains("authorization: bearer test-token"));
            assert!(request.to_ascii_lowercase().contains("content-length: 0"));

            let body = format!(
                r#"{{"stream":{{"url":"ws://{ws_addr}/v1/markets/events","sessionid":"mock-session"}}}}"#
            );
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).unwrap();
        });

        (format!("http://{http_addr}"), payload_receiver)
    }

    fn spawn_reconnecting_mock_tradier_stream() -> (String, mpsc::Receiver<String>, Arc<AtomicUsize>)
    {
        let (payload_sender, payload_receiver) = mpsc::channel();

        let ws_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let ws_addr = ws_listener.local_addr().unwrap();
        ws_listener.set_nonblocking(true).unwrap();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let listener = tokio::net::TcpListener::from_std(ws_listener).unwrap();
                for attempt in 0..2 {
                    let (stream, _) = listener.accept().await.unwrap();
                    let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
                    if let Some(Ok(Message::Text(payload))) = socket.next().await {
                        payload_sender.send(payload).unwrap();
                    }
                    if attempt == 0 {
                        continue;
                    }
                    socket
                        .send(Message::Text(
                            r#"{"type":"quote","symbol":"SPY","bid":"450.10","ask":"450.12","bidsz":"7","asksz":"8","biddate":"1557757189000","askdate":"1557757190000"}"#
                                .to_string(),
                        ))
                        .await
                        .unwrap();
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            });
        });

        let http_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let http_addr = http_listener.local_addr().unwrap();
        let session_requests = Arc::new(AtomicUsize::new(0));
        let session_requests_thread = session_requests.clone();
        std::thread::spawn(move || {
            let (mut stream, _) = http_listener.accept().unwrap();
            session_requests_thread.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mut request = [0_u8; 4096];
            let bytes = stream.read(&mut request).unwrap();
            let request = String::from_utf8_lossy(&request[..bytes]);
            assert!(request.starts_with("POST /markets/events/session "));
            assert!(request
                .to_ascii_lowercase()
                .contains("authorization: bearer test-token"));

            let body = format!(
                r#"{{"stream":{{"url":"ws://{ws_addr}/v1/markets/events","sessionid":"mock-session"}}}}"#
            );
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).unwrap();
        });

        (
            format!("http://{http_addr}"),
            payload_receiver,
            session_requests,
        )
    }
}
