use std::collections::{BTreeSet, HashMap};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread::JoinHandle;
use std::time::Duration;

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
use reqwest::Client;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::json;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};

const LIVE_BASE: &str = "https://api.tradier.com/v1";
const SANDBOX_BASE: &str = "https://sandbox.tradier.com/v1";
const DEFAULT_MARKET_WS_URL: &str = "wss://ws.tradier.com/v1/markets/events";
const MARKET_EVENT_FILTER: [&str; 4] = ["quote", "trade", "timesale", "tradex"];

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
        let access_token = config_string(config, "access_token")
            .or_else(|| config_string(config, "tradier_access_token"))
            .or_else(|| config_string(config, "tradier-access-token"))
            .or_else(|| std::env::var("TRADIER_ACCESS_TOKEN").ok())
            .context("missing access_token")?;

        let use_sandbox = parse_sandbox(config);
        let base_url = config_string(config, "base_url")
            .or_else(|| config_string(config, "tradier_base_url"))
            .or_else(|| config_string(config, "tradier-base-url"))
            .unwrap_or_else(|| {
                if use_sandbox {
                    SANDBOX_BASE.to_string()
                } else {
                    LIVE_BASE.to_string()
                }
            });
        let valid_only = config["valid_only"].as_bool().unwrap_or(true);
        let linebreak = config["linebreak"].as_bool().unwrap_or(true);
        let reconnect_delay = config["reconnect_delay_seconds"]
            .as_u64()
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(3));

        Ok(Self {
            access_token,
            use_sandbox,
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
                .spawn(move || {
                    let runtime = tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(2)
                        .enable_all()
                        .thread_name("tradier-live-worker")
                        .build();
                    match runtime {
                        Ok(runtime) => runtime.block_on(run_tradier_stream(config, state, stop)),
                        Err(error) => error!("Tradier live provider failed to start: {error}"),
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
    while !stop.load(Ordering::Relaxed) {
        match run_tradier_stream_once(&config, &state, &stop).await {
            Ok(()) => {}
            Err(error) => {
                set_connected(&state, false);
                if !stop.load(Ordering::Relaxed) {
                    let error_text = format!("{error:#}");
                    warn!("Tradier live stream disconnected: {error_text}");
                    if is_session_auth_error(&error_text) {
                        fanout_error(&state, error_text);
                        return;
                    }
                    tokio::time::sleep(config.reconnect_delay).await;
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
) -> Result<()> {
    wait_for_subscribers(state, stop).await;
    if stop.load(Ordering::Relaxed) {
        return Ok(());
    }

    let session = create_market_session(config).await?;
    let ws_url = tradier_websocket_url(&session.stream.url);
    let (mut socket, _) = tokio_tungstenite::connect_async(&ws_url)
        .await
        .with_context(|| format!("failed to connect Tradier websocket {ws_url}"))?;
    set_connected(state, true);
    info!("Tradier live websocket connected: {ws_url}");

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
                    let payload = json!({
                        "symbols": signature.symbols,
                        "filter": MARKET_EVENT_FILTER,
                        "sessionid": session.stream.sessionid,
                        "linebreak": config.linebreak,
                        "validOnly": config.valid_only,
                    });
                    socket
                        .send(Message::Text(payload.to_string()))
                        .await
                        .context("failed to send Tradier subscription payload")?;
                    last_signature = signature;
                }
            }
            message = socket.next() => {
                let Some(message) = message else {
                    bail!("Tradier websocket closed");
                };
                match message? {
                    Message::Text(text) => dispatch_text_message(state, &text),
                    Message::Binary(bytes) => {
                        if let Ok(text) = String::from_utf8(bytes) {
                            dispatch_text_message(state, &text);
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

fn set_connected(state: &Arc<Mutex<TradierLiveState>>, connected: bool) {
    if let Ok(mut state) = state.lock() {
        state.connected = connected;
    }
}

async fn create_market_session(config: &TradierLiveConfig) -> Result<TradierSessionResponse> {
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
        .send()
        .await?;
    let status = response.status();
    if status == 401 {
        bail!("Tradier streaming session unauthorized");
    }
    if status == 403 {
        bail!("Tradier streaming session forbidden");
    }
    if !status.is_success() {
        bail!("Tradier streaming session failed with HTTP {status}");
    }
    Ok(response.json::<TradierSessionResponse>().await?)
}

#[derive(Debug, Deserialize)]
struct TradierSessionResponse {
    stream: TradierSession,
}

#[derive(Debug, Deserialize)]
struct TradierSession {
    #[serde(default)]
    url: String,
    sessionid: String,
}

fn dispatch_text_message(state: &Arc<Mutex<TradierLiveState>>, text: &str) {
    for line in text.lines().map(str::trim).filter(|line| !line.is_empty()) {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(line) {
            if let Some(error) = value["error"].as_str() {
                warn!("Tradier stream error: {error}");
                fanout_error(state, format!("Tradier stream error: {error}"));
                continue;
            }
        }
        match serde_json::from_str::<TradierStreamEvent>(line) {
            Ok(event) => dispatch_event(state, event),
            Err(error) => debug!("ignoring Tradier websocket payload: {error}: {line}"),
        }
    }
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

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum TradierStreamEvent {
    Quote {
        symbol: String,
        #[serde(default)]
        bid: FlexibleDecimal,
        #[serde(default)]
        ask: FlexibleDecimal,
        #[serde(default)]
        bidsz: FlexibleDecimal,
        #[serde(default)]
        asksz: FlexibleDecimal,
        #[serde(default)]
        biddate: FlexibleI64,
        #[serde(default)]
        askdate: FlexibleI64,
    },
    Trade {
        symbol: String,
        #[serde(default)]
        exch: Option<String>,
        #[serde(default)]
        price: FlexibleDecimal,
        #[serde(default)]
        size: FlexibleDecimal,
        #[serde(default)]
        date: FlexibleI64,
        #[serde(default)]
        last: FlexibleDecimal,
    },
    Tradex {
        symbol: String,
        #[serde(default)]
        exch: Option<String>,
        #[serde(default)]
        price: FlexibleDecimal,
        #[serde(default)]
        size: FlexibleDecimal,
        #[serde(default)]
        date: FlexibleI64,
        #[serde(default)]
        last: FlexibleDecimal,
    },
    Timesale {
        symbol: String,
        #[serde(default)]
        exch: Option<String>,
        #[serde(default)]
        last: FlexibleDecimal,
        #[serde(default)]
        size: FlexibleDecimal,
        #[serde(default)]
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

    let time = quote_time(*biddate, *askdate);
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
            last,
            ..
        }
        | TradierStreamEvent::Tradex {
            price,
            size,
            date,
            exch,
            last,
            ..
        } => {
            let price = if price.0 > Decimal::ZERO {
                price.0
            } else {
                last.0
            };
            (price, size.0, *date, exch.clone(), None, false)
        }
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
        return None;
    }

    let time = date_time(date);
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

fn positive_bar(price: Decimal) -> Option<Bar> {
    (price > Decimal::ZERO).then(|| Bar::from_price(price))
}

fn quote_time(biddate: FlexibleI64, askdate: FlexibleI64) -> DateTime {
    let millis = biddate.0.max(askdate.0);
    if millis > 0 {
        DateTime::from_millis(millis)
    } else {
        DateTime::now()
    }
}

fn date_time(date: FlexibleI64) -> DateTime {
    if date.0 > 0 {
        DateTime::from_millis(date.0)
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

#[derive(Debug, Clone, Copy, Default)]
struct FlexibleDecimal(Decimal);

impl<'de> Deserialize<'de> for FlexibleDecimal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        Ok(Self(match value {
            serde_json::Value::Number(number) => number
                .as_f64()
                .and_then(Decimal::from_f64)
                .unwrap_or(Decimal::ZERO),
            serde_json::Value::String(value) => value.parse().unwrap_or(Decimal::ZERO),
            _ => Decimal::ZERO,
        }))
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct FlexibleI64(i64);

impl<'de> Deserialize<'de> for FlexibleI64 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        Ok(Self(match value {
            serde_json::Value::Number(number) => number.as_i64().unwrap_or_default(),
            serde_json::Value::String(value) => value.parse().unwrap_or_default(),
            _ => 0,
        }))
    }
}

fn config_string(config: &serde_json::Value, key: &str) -> Option<String> {
    config[key]
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn parse_sandbox(config: &serde_json::Value) -> bool {
    if let Some(value) = config["use_sandbox"]
        .as_bool()
        .or_else(|| config["sandbox"].as_bool())
    {
        return value;
    }
    if let Some(environment) = config_string(config, "environment")
        .or_else(|| config_string(config, "tradier_environment"))
        .or_else(|| config_string(config, "tradier-environment"))
        .or_else(|| std::env::var("TRADIER_ENVIRONMENT").ok())
    {
        return matches!(
            environment.trim().to_ascii_lowercase().as_str(),
            "sandbox" | "paper" | "test"
        );
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use lean_core::{Market, OptionRight, OptionStyle};
    use rust_decimal_macros::dec;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;

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
}
