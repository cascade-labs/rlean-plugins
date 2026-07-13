use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

use anyhow::{Context, Result};
use hyperliquid_rust_sdk::{BaseUrl, InfoClient, Message, Subscription};
use lean_core::{
    LeanError, Market, NanosecondTimestamp, Resolution, SecurityType, Symbol, TickType, TimeSpan,
};
use lean_data::{
    live_data_channel, Bar, CustomDataPoint, DataQueueHandler, LiveDataItem, LiveDataSubscription,
    LiveDataSubscriptionConfig, LiveNodePacket, LiveUniverseSubscriptionConfig, OrderBook,
    OrderBookLevel, QuoteBar, SubscriptionDataConfig, SubscriptionDataKind, TradeBar, TradeBarData,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::history_provider::HyperliquidInfoClient;

const DEFAULT_INFO_URL: &str = "https://api.hyperliquid.xyz/info";
#[derive(Debug, Clone)]
pub struct HyperliquidLiveConfig {
    pub info_url: String,
    pub poll_interval: Duration,
}

impl HyperliquidLiveConfig {
    pub fn from_json(config: &Value) -> Self {
        let info_url = config["info_url"]
            .as_str()
            .map(str::to_string)
            .or_else(|| std::env::var("HYPERLIQUID_INFO_URL").ok())
            .unwrap_or_else(|| DEFAULT_INFO_URL.to_string());
        let poll_secs = config["live_poll_seconds"]
            .as_u64()
            .or_else(|| {
                std::env::var("HYPERLIQUID_LIVE_POLL_SECONDS")
                    .ok()
                    .and_then(|raw| raw.parse().ok())
            })
            .unwrap_or(60);
        Self {
            info_url,
            poll_interval: Duration::from_secs(poll_secs.max(1)),
        }
    }
}

type LiveSender = crossbeam_channel::Sender<lean_core::Result<LiveDataItem>>;

#[derive(Clone)]
struct MarketSubscriber {
    id: u64,
    tick_type: TickType,
    sender: LiveSender,
}

struct MarketWorkerHandle {
    commands: mpsc::UnboundedSender<MarketCommand>,
}

enum MarketCommand {
    Subscribe {
        config: SubscriptionDataConfig,
        subscriber: MarketSubscriber,
    },
    Unsubscribe {
        config: SubscriptionDataConfig,
    },
}

struct MarketStreamState {
    symbol: Symbol,
    resolution: Resolution,
    subscribers: Vec<MarketSubscriber>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum UnderlyingSubscriptionKey {
    Candle { coin: String, interval: String },
    L2Book { coin: String },
}

struct UnderlyingSubscriptionState {
    subscription_id: u32,
    refs: usize,
}

#[derive(Default)]
struct MarketWorkerState {
    streams: HashMap<String, MarketStreamState>,
    underlying: HashMap<UnderlyingSubscriptionKey, UnderlyingSubscriptionState>,
}

#[derive(Default)]
struct RoutedLiveItems {
    trade: Vec<LiveDataItem>,
    quote: Vec<LiveDataItem>,
}

pub struct HyperliquidLiveDataProvider {
    config: HyperliquidLiveConfig,
    connected: Arc<AtomicBool>,
    market_worker: Arc<Mutex<Option<MarketWorkerHandle>>>,
    custom_stops: Arc<Mutex<HashMap<String, Arc<AtomicBool>>>>,
    universe_stops: Arc<Mutex<HashMap<String, Arc<AtomicBool>>>>,
}

impl HyperliquidLiveDataProvider {
    pub fn new(config: HyperliquidLiveConfig) -> Self {
        Self {
            config,
            connected: Arc::new(AtomicBool::new(false)),
            market_worker: Arc::new(Mutex::new(None)),
            custom_stops: Arc::new(Mutex::new(HashMap::new())),
            universe_stops: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn ensure_market_worker(&self) -> lean_core::Result<mpsc::UnboundedSender<MarketCommand>> {
        let mut worker = self.market_worker.lock().unwrap();
        if let Some(handle) = worker.as_ref() {
            return Ok(handle.commands.clone());
        }

        let (command_sender, command_receiver) = mpsc::unbounded_channel();
        let connected = self.connected.clone();
        std::thread::Builder::new()
            .name("hyperliquid-live-market-worker".to_string())
            .spawn(move || {
                let result = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .thread_name("hyperliquid-live-worker")
                    .build()
                    .context("failed to build Hyperliquid live runtime")
                    .and_then(|runtime| {
                        runtime.block_on(run_market_worker(command_receiver, connected))
                    });
                if let Err(error) = result {
                    warn!("Hyperliquid market live worker stopped with error: {error:#}");
                }
            })
            .map_err(|error| LeanError::DataError(error.to_string()))?;

        *worker = Some(MarketWorkerHandle {
            commands: command_sender.clone(),
        });
        Ok(command_sender)
    }

    fn subscribe_custom_config(
        &mut self,
        config: &SubscriptionDataConfig,
    ) -> lean_core::Result<LiveDataSubscription> {
        let custom = config.custom.as_ref().ok_or_else(|| {
            LeanError::DataError("custom live subscription missing custom metadata".to_string())
        })?;
        if custom.source_type != "hyperliquid" {
            return Err(LeanError::Unsupported(format!(
                "Hyperliquid live provider does not support custom source {}",
                custom.source_type
            )));
        }

        let key = format!("{}:{}", custom.source_type, custom.ticker);
        let stop = Arc::new(AtomicBool::new(false));
        self.custom_stops
            .lock()
            .unwrap()
            .insert(key.clone(), stop.clone());
        let (sender, receiver) = live_data_channel();
        let error_sender = sender.clone();
        let live_config = self.config.clone();
        let subscription = config.clone();
        let thread_ticker = custom.ticker.clone();

        std::thread::Builder::new()
            .name(format!("hyperliquid-custom-live-{thread_ticker}"))
            .spawn(move || {
                if let Err(error) =
                    poll_custom_subscription(subscription, live_config, sender, stop)
                {
                    let _ = error_sender.send(Err(LeanError::DataError(error.to_string())));
                    warn!("Hyperliquid custom live subscription stopped with error: {error:#}");
                }
            })
            .map_err(|error| LeanError::DataError(error.to_string()))?;

        Ok(LiveDataSubscription::new(
            LiveDataSubscriptionConfig::Market(Box::new(config.clone())),
            receiver,
        ))
    }
}

impl DataQueueHandler for HyperliquidLiveDataProvider {
    fn set_job(&mut self, _job: &LiveNodePacket) -> lean_core::Result<()> {
        Ok(())
    }

    fn subscribe(
        &mut self,
        config: &SubscriptionDataConfig,
    ) -> lean_core::Result<LiveDataSubscription> {
        if config.data_kind == SubscriptionDataKind::Custom {
            return self.subscribe_custom_config(config);
        }

        if config.symbol.market().as_str() != Market::HYPERLIQUID {
            return Err(LeanError::Unsupported(format!(
                "Hyperliquid live provider does not support market {} for {}",
                config.symbol.market(),
                config.symbol
            )));
        }
        if !matches!(
            config.symbol.security_type(),
            SecurityType::Crypto | SecurityType::CryptoFuture
        ) {
            return Err(LeanError::Unsupported(format!(
                "Hyperliquid live provider only supports crypto symbols, got {:?}",
                config.symbol.security_type()
            )));
        }

        let (sender, receiver) = live_data_channel();
        let subscriber = MarketSubscriber {
            id: config.unique_id(),
            tick_type: config.tick_type,
            sender,
        };
        self.ensure_market_worker()?
            .send(MarketCommand::Subscribe {
                config: config.clone(),
                subscriber,
            })
            .map_err(|error| LeanError::DataError(error.to_string()))?;

        Ok(LiveDataSubscription::new(
            LiveDataSubscriptionConfig::Market(Box::new(config.clone())),
            receiver,
        ))
    }

    fn subscribe_universe(
        &mut self,
        subscription: &LiveUniverseSubscriptionConfig,
    ) -> lean_core::Result<LiveDataSubscription> {
        if subscription.source_type != "hyperliquid" {
            return Err(LeanError::Unsupported(format!(
                "Hyperliquid live provider does not support universe source {}",
                subscription.source_type
            )));
        }

        let key = format!("{}:{}", subscription.source_type, subscription.ticker);
        let stop = Arc::new(AtomicBool::new(false));
        self.universe_stops
            .lock()
            .unwrap()
            .insert(key.clone(), stop.clone());
        let (sender, receiver) = live_data_channel();
        let error_sender = sender.clone();
        let live_config = self.config.clone();
        let subscription_clone = subscription.clone();

        std::thread::Builder::new()
            .name(format!("hyperliquid-universe-live-{}", subscription.ticker))
            .spawn(move || {
                if let Err(error) =
                    poll_universe_subscription(subscription_clone, live_config, sender, stop)
                {
                    let _ = error_sender.send(Err(LeanError::DataError(error.to_string())));
                    warn!("Hyperliquid universe live subscription stopped with error: {error:#}");
                }
            })
            .map_err(|error| LeanError::DataError(error.to_string()))?;

        Ok(LiveDataSubscription::new(
            LiveDataSubscriptionConfig::Universe(subscription.clone()),
            receiver,
        ))
    }

    fn unsubscribe(&mut self, config: &SubscriptionDataConfig) -> lean_core::Result<()> {
        if config.data_kind == SubscriptionDataKind::Custom {
            if let Some(custom) = config.custom.as_ref() {
                let key = format!("{}:{}", custom.source_type, custom.ticker);
                if let Some(stop) = self.custom_stops.lock().unwrap().remove(&key) {
                    stop.store(true, Ordering::Relaxed);
                }
            }
            return Ok(());
        }

        self.ensure_market_worker()?
            .send(MarketCommand::Unsubscribe {
                config: config.clone(),
            })
            .map_err(|error| LeanError::DataError(error.to_string()))?;
        Ok(())
    }

    fn unsubscribe_universe(
        &mut self,
        subscription: &LiveUniverseSubscriptionConfig,
    ) -> lean_core::Result<()> {
        let key = format!("{}:{}", subscription.source_type, subscription.ticker);
        if let Some(stop) = self.universe_stops.lock().unwrap().remove(&key) {
            stop.store(true, Ordering::Relaxed);
        }
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    fn name(&self) -> &str {
        "HyperliquidLiveDataProvider"
    }
}

fn market_stream_key(config: &SubscriptionDataConfig) -> String {
    format!(
        "{}:{}:{:?}:{:?}",
        config.symbol.market().as_str(),
        config.symbol.value,
        config.resolution,
        config.tick_type
    )
}

fn hyperliquid_wire_coin(symbol: &Symbol) -> String {
    if let Some((dex, coin)) = symbol.value.split_once(':') {
        format!("{}:{coin}", dex.to_ascii_lowercase())
    } else {
        symbol.value.to_string()
    }
}

async fn run_market_worker(
    mut commands: mpsc::UnboundedReceiver<MarketCommand>,
    connected: Arc<AtomicBool>,
) -> Result<()> {
    let mut info = InfoClient::with_reconnect(None, Some(BaseUrl::Mainnet)).await?;
    let (message_tx, mut message_rx) = mpsc::unbounded_channel();
    let mut state = MarketWorkerState::default();
    connected.store(true, Ordering::Relaxed);

    loop {
        tokio::select! {
            Some(command) = commands.recv() => {
                if let Err(error) = handle_market_command(command, &mut info, &message_tx, &mut state).await {
                    warn!("Hyperliquid live market command failed: {error:#}");
                }
            }
            Some(message) = message_rx.recv() => {
                dispatch_market_message(&mut state, message);
            }
            else => break,
        }
    }

    connected.store(false, Ordering::Relaxed);
    Ok(())
}

async fn handle_market_command(
    command: MarketCommand,
    info: &mut InfoClient,
    message_tx: &mpsc::UnboundedSender<Message>,
    state: &mut MarketWorkerState,
) -> Result<()> {
    match command {
        MarketCommand::Subscribe { config, subscriber } => {
            let key = market_stream_key(&config);
            let is_new_stream = !state.streams.contains_key(&key);
            let stream = state
                .streams
                .entry(key)
                .or_insert_with(|| MarketStreamState {
                    symbol: config.symbol.clone(),
                    resolution: config.resolution,
                    subscribers: Vec::new(),
                });
            stream
                .subscribers
                .retain(|existing| existing.id != subscriber.id);
            stream.subscribers.push(subscriber);
            if is_new_stream {
                retain_underlying_for_stream(info, message_tx, state, &config).await?;
            }
        }
        MarketCommand::Unsubscribe { config } => {
            let key = market_stream_key(&config);
            let should_release = if let Some(stream) = state.streams.get_mut(&key) {
                stream
                    .subscribers
                    .retain(|subscriber| subscriber.id != config.unique_id());
                stream.subscribers.is_empty()
            } else {
                false
            };
            if should_release {
                state.streams.remove(&key);
                release_underlying_for_stream(info, state, &config).await?;
            }
        }
    }
    Ok(())
}

async fn retain_underlying_for_stream(
    info: &mut InfoClient,
    message_tx: &mpsc::UnboundedSender<Message>,
    state: &mut MarketWorkerState,
    config: &SubscriptionDataConfig,
) -> Result<()> {
    for key in underlying_subscription_keys(config) {
        if let Some(existing) = state.underlying.get_mut(&key) {
            existing.refs += 1;
            continue;
        }
        let subscription_id = info
            .subscribe(key.to_subscription(), message_tx.clone())
            .await?;
        state.underlying.insert(
            key,
            UnderlyingSubscriptionState {
                subscription_id,
                refs: 1,
            },
        );
    }
    Ok(())
}

async fn release_underlying_for_stream(
    info: &mut InfoClient,
    state: &mut MarketWorkerState,
    config: &SubscriptionDataConfig,
) -> Result<()> {
    for key in underlying_subscription_keys(config) {
        let should_unsubscribe = if let Some(existing) = state.underlying.get_mut(&key) {
            existing.refs = existing.refs.saturating_sub(1);
            existing.refs == 0
        } else {
            false
        };
        if should_unsubscribe {
            if let Some(existing) = state.underlying.remove(&key) {
                info.unsubscribe(existing.subscription_id).await?;
            }
        }
    }
    Ok(())
}

fn underlying_subscription_keys(config: &SubscriptionDataConfig) -> Vec<UnderlyingSubscriptionKey> {
    let coin = hyperliquid_wire_coin(&config.symbol);
    let mut keys = Vec::new();
    match config.tick_type {
        TickType::Trade => {
            if let Some(interval) = hyperliquid_interval(config.resolution) {
                keys.push(UnderlyingSubscriptionKey::Candle {
                    coin,
                    interval: interval.to_string(),
                });
            }
        }
        TickType::Quote => {
            keys.push(UnderlyingSubscriptionKey::L2Book { coin });
        }
        TickType::OpenInterest => {}
    }
    keys
}

impl UnderlyingSubscriptionKey {
    fn to_subscription(&self) -> Subscription {
        match self {
            UnderlyingSubscriptionKey::Candle { coin, interval } => Subscription::Candle {
                coin: coin.clone(),
                interval: interval.clone(),
            },
            UnderlyingSubscriptionKey::L2Book { coin } => {
                Subscription::L2Book { coin: coin.clone() }
            }
        }
    }
}

fn dispatch_market_message(state: &mut MarketWorkerState, message: Message) {
    let keys: Vec<_> = state
        .streams
        .iter()
        .filter(|(_, stream)| message_matches_stream(&message, stream))
        .map(|(key, _)| key.clone())
        .collect();

    for key in keys {
        if let Some(stream) = state.streams.get_mut(&key) {
            let routed = message_to_live_items(&stream.symbol, stream.resolution, message.clone());
            fanout_live_items(&mut stream.subscribers, routed);
        }
    }
}

fn message_matches_stream(message: &Message, stream: &MarketStreamState) -> bool {
    let coin = hyperliquid_wire_coin(&stream.symbol);
    match message {
        Message::Candle(candle) => {
            candle.data.coin == coin
                && hyperliquid_interval(stream.resolution) == Some(candle.data.interval.as_str())
        }
        Message::L2Book(book) => book.data.coin == coin,
        _ => false,
    }
}

fn fanout_live_items(subscribers: &mut Vec<MarketSubscriber>, routed: RoutedLiveItems) -> bool {
    subscribers.retain(|subscriber| {
        let items = match subscriber.tick_type {
            TickType::Trade => &routed.trade,
            TickType::Quote | TickType::OpenInterest => &routed.quote,
        };
        let mut alive = true;
        for item in items {
            if subscriber.sender.send(Ok(item.clone())).is_err() {
                alive = false;
                break;
            }
        }
        alive
    });
    !subscribers.is_empty()
}

fn message_to_live_items(
    symbol: &Symbol,
    resolution: Resolution,
    message: Message,
) -> RoutedLiveItems {
    match message {
        Message::Candle(candle) => RoutedLiveItems {
            trade: candle_to_trade_bar(symbol, resolution, &candle.data)
                .map(LiveDataItem::TradeBar)
                .into_iter()
                .collect(),
            quote: Vec::new(),
        },
        Message::L2Book(book) => {
            let order_book = l2_to_order_book(symbol, &book.data);
            let mut quote = Vec::new();
            if let Some(quote_bar) = order_book_to_quote_bar(&order_book, resolution) {
                quote.push(LiveDataItem::QuoteBar(quote_bar));
            }
            quote.push(LiveDataItem::OrderBook(order_book));
            RoutedLiveItems {
                trade: Vec::new(),
                quote,
            }
        }
        Message::NoData | Message::HyperliquidError(_) => RoutedLiveItems {
            trade: Vec::new(),
            quote: vec![LiveDataItem::Heartbeat(NanosecondTimestamp::now())],
        },
        other => {
            debug!("ignoring Hyperliquid websocket message: {other:?}");
            RoutedLiveItems::default()
        }
    }
}

fn candle_to_trade_bar(
    symbol: &Symbol,
    resolution: Resolution,
    candle: &hyperliquid_rust_sdk::CandleData,
) -> Option<TradeBar> {
    let time = NanosecondTimestamp::from_millis(candle.time_open as i64);
    let period = resolution.to_time_span().unwrap_or_else(|| {
        TimeSpan::from_millis((candle.time_close.saturating_sub(candle.time_open)) as i64)
    });
    Some(TradeBar::new(
        symbol.clone(),
        time,
        period,
        TradeBarData::new(
            parse_decimal(&candle.open)?,
            parse_decimal(&candle.high)?,
            parse_decimal(&candle.low)?,
            parse_decimal(&candle.close)?,
            parse_decimal(&candle.volume)?,
        ),
    ))
}

fn l2_to_order_book(symbol: &Symbol, book: &hyperliquid_rust_sdk::L2BookData) -> OrderBook {
    let time = NanosecondTimestamp::from_millis(book.time as i64);
    let bids = book
        .levels
        .first()
        .into_iter()
        .flatten()
        .filter_map(book_level_to_order_book_level)
        .collect();
    let asks = book
        .levels
        .get(1)
        .into_iter()
        .flatten()
        .filter_map(book_level_to_order_book_level)
        .collect();
    OrderBook::new(symbol.clone(), time, bids, asks)
}

fn order_book_to_quote_bar(book: &OrderBook, resolution: Resolution) -> Option<QuoteBar> {
    let bid = book.best_bid()?;
    let ask = book.best_ask()?;
    let period = resolution.to_time_span().unwrap_or(TimeSpan::ONE_SECOND);
    let time = NanosecondTimestamp(book.time.0 - book.time.0.rem_euclid(period.nanos));
    Some(QuoteBar::new(
        book.symbol.clone(),
        time,
        period,
        Some(Bar::from_price(bid.price)),
        Some(Bar::from_price(ask.price)),
        bid.size,
        ask.size,
    ))
}

fn book_level_to_order_book_level(
    level: &hyperliquid_rust_sdk::BookLevel,
) -> Option<OrderBookLevel> {
    Some(OrderBookLevel::new(
        parse_decimal(&level.px)?,
        parse_decimal(&level.sz)?,
        level.n.try_into().unwrap_or(u32::MAX),
    ))
}

fn poll_custom_subscription(
    subscription: SubscriptionDataConfig,
    config: HyperliquidLiveConfig,
    sender: crossbeam_channel::Sender<lean_core::Result<LiveDataItem>>,
    stop: Arc<AtomicBool>,
) -> Result<()> {
    let custom = subscription
        .custom
        .as_ref()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("custom live subscription missing custom metadata"))?;
    let info = HyperliquidInfoClient::new(config.info_url);
    let mut consecutive_errors = 0u32;
    while !stop.load(Ordering::Relaxed) {
        let points = match load_live_custom_points(&info, &custom.ticker, &custom.config.properties)
        {
            Ok(points) => {
                consecutive_errors = 0;
                points
            }
            Err(error) if is_retriable_live_custom_error(&error) => {
                consecutive_errors = consecutive_errors.saturating_add(1);
                let backoff = live_custom_error_backoff(config.poll_interval, consecutive_errors);
                warn!(
                    "Hyperliquid live custom poll {}:{} failed with retriable error; backing off for {:?}: {error:#}",
                    custom.source_type,
                    custom.ticker,
                    backoff
                );
                sleep_until_stopped(&stop, backoff);
                continue;
            }
            Err(error) => return Err(error),
        };
        tracing::info!(
            "Hyperliquid live custom poll {}:{} points={}",
            custom.source_type,
            custom.ticker,
            points.len()
        );
        for point in points {
            if sender
                .send(Ok(LiveDataItem::CustomData {
                    symbol: subscription.symbol.clone(),
                    source_type: custom.source_type.clone(),
                    ticker: custom.ticker.clone(),
                    point,
                }))
                .is_err()
            {
                return Ok(());
            }
        }
        sleep_until_stopped(&stop, config.poll_interval);
    }
    Ok(())
}

fn poll_universe_subscription(
    subscription: LiveUniverseSubscriptionConfig,
    config: HyperliquidLiveConfig,
    sender: crossbeam_channel::Sender<lean_core::Result<LiveDataItem>>,
    stop: Arc<AtomicBool>,
) -> Result<()> {
    let info = HyperliquidInfoClient::new(config.info_url);
    let mut consecutive_errors = 0u32;
    let poll_interval = live_universe_poll_interval(config.poll_interval, subscription.resolution);
    while !stop.load(Ordering::Relaxed) {
        let points = match load_live_universe_points(&info, &subscription) {
            Ok(points) => {
                consecutive_errors = 0;
                points
            }
            Err(error) if is_retriable_live_custom_error(&error) => {
                consecutive_errors = consecutive_errors.saturating_add(1);
                let backoff = live_custom_error_backoff(poll_interval, consecutive_errors);
                warn!(
                    "Hyperliquid live universe poll {}:{} failed with retriable error; backing off for {:?}: {error:#}",
                    subscription.source_type,
                    subscription.ticker,
                    backoff
                );
                sleep_until_stopped(&stop, backoff);
                continue;
            }
            Err(error) => return Err(error),
        };
        let time = points
            .iter()
            .map(|point| point.end_time)
            .max_by_key(|time| time.0)
            .unwrap_or_else(NanosecondTimestamp::now);
        tracing::info!(
            "Hyperliquid live universe poll {}:{} resolution={:?} points={}",
            subscription.source_type,
            subscription.ticker,
            subscription.resolution,
            points.len()
        );
        if sender
            .send(Ok(LiveDataItem::UniverseData {
                source_type: subscription.source_type.clone(),
                ticker: subscription.ticker.clone(),
                resolution: subscription.resolution,
                time,
                data: points,
            }))
            .is_err()
        {
            return Ok(());
        }
        sleep_until_stopped(&stop, poll_interval);
    }
    Ok(())
}

fn live_universe_poll_interval(configured: Duration, resolution: Resolution) -> Duration {
    let resolution_interval = resolution
        .to_time_span()
        .map(|span| Duration::from_nanos(span.nanos.max(0) as u64))
        .unwrap_or(configured);
    configured
        .max(resolution_interval)
        .max(Duration::from_secs(1))
}

fn is_retriable_live_custom_error(error: &anyhow::Error) -> bool {
    // Format the full context chain — the retriable signal (status code or
    // API-call context) can sit at any level, not just the outermost message.
    let message = format!("{error:#}").to_ascii_lowercase();
    message.contains("http 429")
        || message.contains("http 500")
        || message.contains("http 502")
        || message.contains("http 503")
        || message.contains("http 504")
        || message.contains("rate limited")
        || message.contains("timeout")
        || message.contains("timed out")
        || message.contains("connection")
        || message.contains("failed to call hyperliquid info api")
        || message.contains("hyperliquid info api returned an error")
}

fn live_custom_error_backoff(base: Duration, consecutive_errors: u32) -> Duration {
    let floor = Duration::from_secs(60);
    let base = base.max(floor);
    let multiplier = 1u32 << consecutive_errors.saturating_sub(1).min(4);
    (base * multiplier).min(Duration::from_secs(15 * 60))
}

fn sleep_until_stopped(stop: &AtomicBool, duration: Duration) {
    let mut remaining = duration;
    while !stop.load(Ordering::Relaxed) && remaining > Duration::ZERO {
        let step = remaining.min(Duration::from_secs(1));
        std::thread::sleep(step);
        remaining = remaining.saturating_sub(step);
    }
}

fn load_live_custom_points(
    info: &HyperliquidInfoClient,
    ticker: &str,
    properties: &HashMap<String, String>,
) -> Result<Vec<CustomDataPoint>> {
    load_live_points(info, ticker, properties)
}

fn load_live_universe_points(
    info: &HyperliquidInfoClient,
    subscription: &LiveUniverseSubscriptionConfig,
) -> Result<Vec<CustomDataPoint>> {
    load_live_points(info, &subscription.ticker, &subscription.properties)
}

fn load_live_points(
    info: &HyperliquidInfoClient,
    ticker: &str,
    properties: &HashMap<String, String>,
) -> Result<Vec<CustomDataPoint>> {
    let universe = ticker.trim().to_ascii_uppercase();
    let dex = universe
        .strip_prefix("HIP3_")
        .map(str::to_string)
        .or_else(|| properties.get("dex").cloned());
    let request_dex = dex.as_deref().map(|value| value.to_ascii_lowercase());
    let response = info.meta_and_asset_ctxs(request_dex.as_deref())?;
    let array = response
        .as_array()
        .filter(|array| array.len() >= 2)
        .ok_or_else(|| {
            anyhow::anyhow!("Hyperliquid metaAndAssetCtxs response must be [meta, ctxs]")
        })?;
    let meta_rows = array[0]
        .get("universe")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let ctx_rows = array[1].as_array().cloned().unwrap_or_default();
    let now = NanosecondTimestamp::now();
    let dex_value = dex.unwrap_or_default().to_ascii_uppercase();

    let mut points = Vec::new();
    for (index, ctx) in ctx_rows.iter().enumerate() {
        let Some(meta) = meta_rows.get(index) else {
            continue;
        };
        let Some(raw_coin) = meta.get("name").and_then(Value::as_str).map(str::trim) else {
            continue;
        };
        let symbol = if !dex_value.is_empty() && !raw_coin.contains(':') {
            format!("{}:{}", dex_value, raw_coin).to_ascii_uppercase()
        } else {
            raw_coin.to_ascii_uppercase()
        };
        let shared = ctx;
        let mid_px = shared
            .get("midPx")
            .or_else(|| shared.get("mid_px"))
            .and_then(parse_json_decimal)
            .or_else(|| shared.get("markPx").and_then(parse_json_decimal))
            .or_else(|| shared.get("mark_px").and_then(parse_json_decimal));
        let mark_px = shared
            .get("markPx")
            .or_else(|| shared.get("mark_px"))
            .and_then(parse_json_decimal);
        let value = mid_px.or(mark_px).unwrap_or(dec!(0));

        let mut fields = HashMap::new();
        fields.insert("symbol".to_string(), json!(symbol));
        fields.insert("coin".to_string(), json!(raw_coin));
        fields.insert(
            "security_type".to_string(),
            json!(SecurityType::CryptoFuture.to_string()),
        );
        fields.insert("market".to_string(), json!(Market::HYPERLIQUID));
        fields.insert("universe".to_string(), json!(universe));
        fields.insert("dex".to_string(), json!(dex_value));
        fields.insert("source".to_string(), json!("asset_ctxs"));
        fields.insert("is_historical".to_string(), json!(true));
        fields.insert("value".to_string(), json!(value.to_string()));
        insert_optional_decimal(&mut fields, "funding", shared.get("funding"));
        insert_optional_decimal(&mut fields, "open_interest", shared.get("openInterest"));
        insert_optional_decimal(&mut fields, "prev_day_px", shared.get("prevDayPx"));
        insert_optional_decimal(&mut fields, "day_ntl_vlm", shared.get("dayNtlVlm"));
        insert_optional_decimal(&mut fields, "oracle_px", shared.get("oraclePx"));
        insert_optional_decimal(&mut fields, "mark_px", shared.get("markPx"));
        insert_optional_decimal(&mut fields, "mid_px", shared.get("midPx"));
        insert_impact_prices(&mut fields, shared);
        fields.insert(
            "max_leverage".to_string(),
            meta.get("maxLeverage")
                .cloned()
                .or_else(|| meta.get("max_leverage").cloned())
                .unwrap_or_else(|| json!(1)),
        );
        fields.insert(
            "sz_decimals".to_string(),
            meta.get("szDecimals").cloned().unwrap_or(Value::Null),
        );
        fields.insert("index".to_string(), json!(index as i64));
        fields.insert("base".to_string(), Value::Null);
        fields.insert("quote".to_string(), json!("USDC"));

        // Intraday snapshot: time == end_time == the poll's own timestamp.
        points
            .push(CustomDataPoint::new(now, now, value, fields).with_symbol(Some(symbol.clone())));
    }
    Ok(points)
}

fn insert_impact_prices(fields: &mut HashMap<String, Value>, ctx: &Value) {
    let mut impact_bid = ctx
        .get("impactBidPx")
        .or_else(|| ctx.get("impact_bid_px"))
        .and_then(parse_json_decimal);
    let mut impact_ask = ctx
        .get("impactAskPx")
        .or_else(|| ctx.get("impact_ask_px"))
        .and_then(parse_json_decimal);

    if impact_bid.is_none() || impact_ask.is_none() {
        if let Some(values) = ctx.get("impactPxs").and_then(Value::as_array) {
            if values.len() >= 2 {
                // Hyperliquid returns [buy impact, sell impact]. Buy impact is
                // the ask fill estimate, sell impact is the bid fill estimate.
                impact_ask = values.first().and_then(parse_json_decimal);
                impact_bid = values.get(1).and_then(parse_json_decimal);
            }
        }
    }

    fields.insert(
        "impact_bid_px".to_string(),
        impact_bid
            .map(|decimal| json!(decimal.to_string()))
            .unwrap_or(Value::Null),
    );
    fields.insert(
        "impact_ask_px".to_string(),
        impact_ask
            .map(|decimal| json!(decimal.to_string()))
            .unwrap_or(Value::Null),
    );
}

fn insert_optional_decimal(fields: &mut HashMap<String, Value>, key: &str, value: Option<&Value>) {
    fields.insert(
        key.to_string(),
        value
            .and_then(parse_json_decimal)
            .map(|decimal| json!(decimal.to_string()))
            .unwrap_or(Value::Null),
    );
}

fn parse_decimal(value: &str) -> Option<Decimal> {
    value.trim().parse::<Decimal>().ok()
}

fn parse_json_decimal(value: &Value) -> Option<Decimal> {
    if let Some(raw) = value.as_str() {
        return parse_decimal(raw);
    }
    if let Some(raw) = value.as_f64() {
        return Decimal::from_f64_retain(raw);
    }
    if let Some(raw) = value.as_i64() {
        return Some(Decimal::from(raw));
    }
    None
}

fn hyperliquid_interval(resolution: Resolution) -> Option<&'static str> {
    match resolution {
        Resolution::Tick => None,
        Resolution::Second | Resolution::Minute => Some("1m"),
        Resolution::Hour => Some("1h"),
        Resolution::Daily => Some("1d"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyperliquid_rust_sdk::{BookLevel, CandleData, L2Book, L2BookData};
    use lean_core::{Market, Resolution, Symbol};

    fn hyperliquid_symbol() -> Symbol {
        Symbol::create_crypto_future("XYZ:SP500", &Market::new(Market::HYPERLIQUID))
    }

    #[test]
    fn wire_coin_lowercases_hip3_dex_prefix_only() {
        assert_eq!(hyperliquid_wire_coin(&hyperliquid_symbol()), "xyz:SP500");
        assert_eq!(
            hyperliquid_wire_coin(&Symbol::create_crypto_future(
                "ETH",
                &Market::new(Market::HYPERLIQUID)
            )),
            "ETH"
        );
    }

    #[test]
    fn market_stream_key_keeps_trade_and_quote_streams_separate() {
        let symbol = hyperliquid_symbol();
        let trade = SubscriptionDataConfig::new_crypto_future(symbol.clone(), Resolution::Minute);
        let mut quote = SubscriptionDataConfig::new_crypto_future(symbol, Resolution::Minute);
        quote.tick_type = TickType::Quote;

        assert_ne!(market_stream_key(&trade), market_stream_key(&quote));
    }

    fn l2_message() -> Message {
        Message::L2Book(L2Book {
            data: L2BookData {
                coin: "XYZ:SP500".to_string(),
                time: 1_765_000_000_000,
                levels: vec![
                    vec![BookLevel {
                        px: "100.00".to_string(),
                        sz: "5.0".to_string(),
                        n: 2,
                    }],
                    vec![BookLevel {
                        px: "100.20".to_string(),
                        sz: "4.0".to_string(),
                        n: 1,
                    }],
                ],
            },
        })
    }

    fn candle_message() -> Message {
        Message::Candle(hyperliquid_rust_sdk::Candle {
            data: CandleData {
                time_close: 1_765_000_060_000,
                close: "101.0".to_string(),
                high: "102.0".to_string(),
                interval: "1m".to_string(),
                low: "99.0".to_string(),
                num_trades: 7,
                open: "100.0".to_string(),
                coin: "XYZ:SP500".to_string(),
                time_open: 1_765_000_000_000,
                volume: "123.0".to_string(),
            },
        })
    }

    #[test]
    fn l2_book_routes_quote_bar_to_quote_subscriptions() {
        let routed = message_to_live_items(&hyperliquid_symbol(), Resolution::Hour, l2_message());

        assert!(routed.trade.is_empty());
        assert_eq!(routed.quote.len(), 2);
        assert!(routed
            .quote
            .iter()
            .any(|item| matches!(item, LiveDataItem::QuoteBar(_))));
        assert!(routed
            .quote
            .iter()
            .any(|item| matches!(item, LiveDataItem::OrderBook(_))));

        let quote_bar = routed
            .quote
            .iter()
            .find_map(|item| match item {
                LiveDataItem::QuoteBar(bar) => Some(bar),
                _ => None,
            })
            .unwrap();
        assert_eq!(quote_bar.mid_close(), dec!(100.10));
        assert_eq!(quote_bar.period, TimeSpan::from_hours(1));
    }

    #[test]
    fn l2_book_routes_full_order_book_to_quote_subscriptions() {
        let routed = message_to_live_items(&hyperliquid_symbol(), Resolution::Minute, l2_message());

        let order_book = routed
            .quote
            .iter()
            .find_map(|item| match item {
                LiveDataItem::OrderBook(book) => Some(book),
                _ => None,
            })
            .unwrap();
        assert_eq!(order_book.best_bid().unwrap().price, dec!(100.00));
        assert_eq!(order_book.best_bid().unwrap().size, dec!(5.0));
        assert_eq!(order_book.best_ask().unwrap().price, dec!(100.20));
        assert_eq!(order_book.best_ask().unwrap().size, dec!(4.0));
    }

    #[test]
    fn candles_route_trade_bar_only_to_trade_subscriptions() {
        let routed =
            message_to_live_items(&hyperliquid_symbol(), Resolution::Minute, candle_message());

        assert_eq!(routed.trade.len(), 1);
        assert!(matches!(routed.trade[0], LiveDataItem::TradeBar(_)));
        assert!(routed.quote.is_empty());
    }

    #[test]
    fn fanout_sends_only_matching_tick_type_items() {
        let (trade_sender, trade_receiver) = live_data_channel();
        let (quote_sender, quote_receiver) = live_data_channel();
        let mut subscribers = vec![
            MarketSubscriber {
                id: 1,
                tick_type: TickType::Trade,
                sender: trade_sender,
            },
            MarketSubscriber {
                id: 2,
                tick_type: TickType::Quote,
                sender: quote_sender,
            },
        ];

        let routed = message_to_live_items(&hyperliquid_symbol(), Resolution::Minute, l2_message());
        assert!(fanout_live_items(&mut subscribers, routed));

        assert!(trade_receiver.try_recv().is_err());
        assert!(matches!(
            quote_receiver.try_recv().unwrap().unwrap(),
            LiveDataItem::QuoteBar(_)
        ));
        assert!(matches!(
            quote_receiver.try_recv().unwrap().unwrap(),
            LiveDataItem::OrderBook(_)
        ));
        assert!(quote_receiver.try_recv().is_err());
    }

    #[test]
    fn live_universe_points_include_impact_prices() {
        let ctx = json!({
            "impactPxs": ["5004.5", "5001.5"]
        });
        let mut fields = HashMap::new();

        insert_impact_prices(&mut fields, &ctx);

        assert_eq!(fields["impact_ask_px"], json!("5004.5"));
        assert_eq!(fields["impact_bid_px"], json!("5001.5"));
    }

    #[test]
    fn live_custom_retry_classifier_treats_info_status_errors_as_retriable() {
        let error = anyhow::anyhow!(
            "Hyperliquid Info API returned an error for {{\"dex\":\"xyz\",\"type\":\"metaAndAssetCtxs\"}}"
        )
        .context("HTTP status 500 Internal Server Error");

        assert!(is_retriable_live_custom_error(&error));
    }

    #[test]
    fn live_custom_retry_classifier_keeps_parse_errors_fatal() {
        let error = anyhow::anyhow!("Hyperliquid metaAndAssetCtxs response must be [meta, ctxs]");

        assert!(!is_retriable_live_custom_error(&error));
    }
}
