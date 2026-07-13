# rlean-plugins

[![Tests](https://github.com/cascade-labs/rlean-plugins/actions/workflows/test.yml/badge.svg)](https://github.com/cascade-labs/rlean-plugins/actions/workflows/test.yml)

Runtime plugins for [rlean](https://github.com/cascade-labs/rlean) — data providers, brokerages, and custom-data sources. This README is the API reference a plugin author reads top to bottom. It describes the contract every plugin must satisfy, and it is verified against the traits in rlean's `lean-plugin`, `lean-data-providers`, and `lean-data` crates.

## Plugin model

A plugin is a Rust `cdylib` crate. rlean loads it at run time from `~/.rlean/plugins/`. There is no compile-time dependency between rlean and any specific broker or data source.

Every plugin exports one descriptor function. The `rlean_plugin!` macro from `lean-plugin` writes it for you:

```rust
use lean_plugin::{rlean_plugin, PluginKind};

rlean_plugin! {
    name    = "massive",
    version = "0.1.0",
    kind    = PluginKind::DataProviderHistorical,
}
```

That expands to the required export:

```rust
#[no_mangle]
pub extern "C" fn rlean_plugin_descriptor() -> PluginDescriptor { ... }
```

rlean calls `rlean_plugin_descriptor` first, reads the `PluginDescriptor`, then looks up the factory symbols for the declared kind.

### PluginDescriptor and PluginKind

Both are `#[repr(C)]`, defined in `lean-plugin`.

```rust
#[repr(C)]
pub struct PluginDescriptor {
    pub name: *const u8,     // null-terminated C string, 'static
    pub version: *const u8,  // null-terminated SemVer C string
    pub kind: PluginKind,
}

#[repr(C)]
pub enum PluginKind {
    Brokerage = 0,
    DataProviderHistorical = 1,
    DataProviderLive = 2,
    CustomData = 3,
    AiSkill = 4,
}
```

The `kind` is the plugin's primary capability. A plugin may export more than one factory (for example a brokerage that also serves history and live quotes); rlean loads whichever factories are present.

### ABI compatibility

The ABI is C-stable via `#[repr(C)]` and thin-pointer factory functions, but there is no runtime version token yet — enforcing `abi_token` in the descriptor is [rlean issue #24](https://github.com/cascade-labs/rlean/issues/24), still open. Until that lands, compatibility is by convention: **build plugins in lockstep with the rlean revision you run them against.** The factory boundary casts opaque pointers to trait objects, so a plugin built against a different revision of a trait (`IHistoryProvider`, `ICustomDataSource`, `Brokerage`, `DataQueueHandler`) will misbehave. The release workflow builds against the matching public rlean checkout for exactly this reason.

### Factory functions

Each kind has a create/destroy pair. Names are looked up by symbol at load time. All create functions take a JSON config C string (except custom data, which is configured through `initialize`) and return a heap pointer that rlean frees with the matching destroy function.

| Kind | Create symbol | Destroy symbol | Returns (boxed) |
|---|---|---|---|
| Historical data | `rlean_create_history_provider` | `rlean_destroy_history_provider` | `Arc<dyn IHistoryProvider>` |
| Live data | `rlean_create_live_data_provider` | `rlean_destroy_live_data_provider` | `Box<dyn DataQueueHandler>` |
| Custom data | `rlean_custom_data_factory` | `rlean_destroy_custom_data_source` | `Arc<dyn ICustomDataSource>` |
| Brokerage | `rlean_create_brokerage` | `rlean_destroy_brokerage` | `Box<dyn Brokerage>` |

## Data provider contract

A data provider implements `IHistoryProvider` from `lean-data-providers`. There are two directions to understand: the calls the engine can make into your provider, and the rows you hand back.

### Direction 1: calls the engine makes

`IHistoryProvider` is one trait with many methods. Only `get_history` is required; everything else has a default that returns an empty result, so you implement only what your source supports. The engine calls these methods; you fetch and return rows.

Each `HistoryRequest` carries a `symbol`, `resolution`, `start`/`end`, and `data_type`.

| Method | Purpose | Returns |
|---|---|---|
| `name()` | Provider name for diagnostics | `&str` |
| `get_history(req)` | Trade bars for the request (required) | `Vec<TradeBar>` |
| `get_quote_bars(req)` | Quote bars | `Vec<QuoteBar>` |
| `get_ticks(req)` | Ticks | `Vec<Tick>` |
| `get_margin_interest_rates(req)` | Margin-interest / funding-rate rows | `Vec<MarginInterestRate>` |
| `get_perpetual_contexts(req)` | Perpetual-future context rows | `Vec<PerpetualContext>` |
| `get_factor_file(symbol)` | Split/dividend adjustment rows | `Vec<FactorFileEntry>` |
| `get_map_file(symbol)` | Ticker rename/listing history | `Vec<MapFileEntry>` |
| `get_history_batch(req)` | Multi-symbol fetch; default fans out over the single-symbol methods | `MarketDataBatch` |
| `get_option_eod_bars(ticker, date)` | End-of-day option bars for an underlying on a date | `Vec<OptionEodBar>` |
| `get_option_universe(ticker, date)` | Contracts listed for an underlying on a date | `Vec<OptionUniverseRow>` |
| `get_option_universes(tickers, date)` | Batch of the above; default fans out | `HashMap<String, Vec<OptionUniverseRow>>` |
| `get_option_trade_bars(ticker, res, date)` | Intraday option trade bars for all contracts | `Vec<TradeBar>` |
| `get_option_trade_bars_filtered(ticker, res, date, contracts)` | Same, limited to a selected contract set | `Vec<TradeBar>` |
| `get_option_quote_bars(...)` / `_filtered(...)` | Intraday option quote bars | `Vec<QuoteBar>` |
| `get_option_ticks(ticker, date)` / `_filtered(...)` | Option ticks | `Vec<Tick>` |
| `stream_option_ticks_filtered(ticker, date, contracts)` | Memory-bounded tick stream in timestamp order | `TickStream` |
| `get_option_history_batch(req)` | Batch option fetch; default fans out | `OptionMarketDataBatch` |
| `earliest_date()` | Lower bound the feed uses to clip requested ranges | `Option<NaiveDate>` |

The `_filtered` option methods take an already-selected `&[OptionUniverseRow]` so a provider with contract-specific endpoints can reduce the remote request surface, not just the delivered rows.

Two contract rules are strict:

- **`get_factor_file` and `get_map_file` must return `Err` on partial failure.** The engine persists any non-empty `Ok` and never refetches once rows exist. If an upstream call needed to build the file fails (a splits/dividends or ticker-events fetch errors or rate-limits), return `Err` — do not synthesize a default file from partial data. A fabricated file poisons the cache permanently: later runs read wrong factors or lose a ticker's rename history (for example FB → META). Only return a default/identity file when every fetch succeeded and the symbol genuinely has no corporate actions.

### Direction 2: what you return, and where it goes

Providers return rows. They never write storage. The rlean engine is the single writer: it takes the rows you return and persists them into the Iceberg tables under `lean-storage`. Do not write files, do not query local storage, and do not keep a provider-side cache of persisted data.

### Live data: the DataQueueHandler pattern

A live data provider implements `DataQueueHandler` from `lean-data` (rlean's equivalent of LEAN's `IDataQueueHandler`).

```rust
pub trait DataQueueHandler: Send + Sync {
    fn set_job(&mut self, job: &LiveNodePacket) -> Result<()> { Ok(()) }
    fn subscribe(&mut self, config: &SubscriptionDataConfig) -> Result<LiveDataSubscription>;
    fn subscribe_universe(&mut self, sub: &LiveUniverseSubscriptionConfig) -> Result<LiveDataSubscription>;
    fn unsubscribe(&mut self, config: &SubscriptionDataConfig) -> Result<()>;
    fn unsubscribe_universe(&mut self, sub: &LiveUniverseSubscriptionConfig) -> Result<()>;
    fn is_connected(&self) -> bool;
    fn name(&self) -> &str { "DataQueueHandler" }
}
```

`subscribe` returns a `LiveDataSubscription` that owns a channel receiver. The handler owns the polling work: it spawns a background worker that hits the source (a Tradier-style REST quote poll, a WebSocket stream, and so on) and pushes decoded `LiveDataItem` values into the channel. The engine is passive — it only receives from the channel and advances the frontier. Handlers stack: returning an "unsupported" error lets the next handler try a subscription.

### Custom-data feeds

A custom-data source implements `ICustomDataSource` from `lean-data-providers`. It returns `CustomDataPoint` values that the engine persists into the `custom_points` Iceberg table.

`CustomDataPoint` mirrors LEAN's two `BaseData` time fields, and **both are required**:

```rust
pub struct CustomDataPoint {
    pub time: DateTime,      // period start (LEAN BaseData.Time)
    pub end_time: DateTime,  // period end / emission gate (LEAN BaseData.EndTime)
    pub value: Decimal,
    pub symbol: Option<String>,
    pub fields: Arc<HashMap<String, serde_json::Value>>,
}
```

`end_time` is the emission gate: a point is never surfaced to an algorithm before its `end_time`. This is the [rlean #81 contract](https://github.com/cascade-labs/rlean/issues/81) — it stops daily data from leaking a day early. Build points with the helpers, do not hand-set times to midnight:

- `CustomDataPoint::daily_eod(time, value, fields)` — the daily / EOD idiom, sets `end_time = time + 1 day`. Use this for FRED, CBOE VIX, and similar daily series.
- `CustomDataPoint::new(time, end_time, value, fields)` — explicit times.
- `CustomDataPoint::with_lean_defaulting(time?, end_time?, value, fields)` — applies LEAN's rules: both set are used as-is; only one set copies to the other; neither set returns `None` (never guess midnight).

`ICustomDataSource` methods:

| Method | Purpose |
|---|---|
| `name()` | Registry id, e.g. `"fred"` |
| `initialize(context)` | Receive engine-owned config and paths |
| `get_source(ticker, date, config)` | Where to fetch data for a ticker on a date (URL/path), or `None` |
| `reader(line, date, config)` | Parse one fetched line into a `CustomDataPoint`; `None` to skip |
| `get_live_points(ticker, utc, config, query)` | Decoded points for a live poll |
| `live_poll_delay(ticker, utc, source_available, config, query)` | How long to sleep before the next live poll |
| `is_full_history_source()` | Source serves the whole series in one download |
| `read_history_line(line, config)` | Parse a line from a full-history file (date comes from the line) |
| `history(ticker, config)` | Return all points directly, for APIs without file sources |
| `history_sources(ticker, config)` | Per-date sources for full-history download and cache |
| `default_resolution()` / `_for_ticker(ticker)` | Subscription default resolution |
| `requires_mapping()` | Whether to resolve ticker renames (usually `false`) |
| `is_parquet_native()` | Source is native Parquet rather than text |

## Config

Per-plugin config lives in `~/.rlean/plugin-configs.json`. The outer key is the plugin name; the inner object is arbitrary key/value pairs that plugin defines. Set values with `rlean config` or edit the file (it is written mode `0600`).

A data/brokerage factory receives its config block as a JSON C string. Read credentials from the config, and fall back to an environment variable if you want. From `massive`:

```rust
let api_key = config["api_key"].as_str()
    .map(str::to_string)
    .or_else(|| std::env::var("MASSIVE_API_KEY").ok());
```

Custom-data sources receive their config through `initialize`, not through a factory JSON argument.

## Building

Plugins depend on rlean crates by relative path (`../rlean/crates/...`), so a **public rlean checkout must sit beside this repo**:

```
code/
  rlean/          # public, cloned anonymously — no deploy keys needed
  rlean-plugins/
```

Normal use is through the rlean CLI, which handles the checkout and build for you:

```sh
rlean plugin install massive
```

`rlean plugin install` clones this repo, builds the requested plugin against a sibling rlean checkout, and copies the resulting dylib into `~/.rlean/plugins/`.

### Releases

Releases follow the same flow as rlean. `scripts/bump-version.sh [patch|minor|major]` bumps the workspace version and opens a `Release v<version>` PR; merging it auto-tags `v<version>` and dispatches the release workflow. The workflow checks out `cascade-labs/rlean` anonymously as a sibling, builds every plugin cdylib for each supported platform, and publishes one `plugin-<name>-<version>-<triple>.tar.gz` per plugin per triple plus a top-level `manifest.json`. Supported triples: `aarch64-apple-darwin`, `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`. `rlean cloud install` pulls the bundle matching each node's triple.

## Plugins in this repo

Copy the closest example when writing a new plugin. `massive` is the fullest data provider; `tradier` is a brokerage that also serves history and live quotes; `fred` and `cboe_vix` are simple custom-data sources.

| Plugin | Path | Kind | Provides |
|---|---|---|---|
| massive | `data_providers/massive` | DataProviderHistorical | Full historical: equity + option bars/ticks, factor files, map files |
| thetadata | `data_providers/thetadata` | DataProviderHistorical | Options-focused historical from a local ThetaData terminal |
| hyperliquid | `data_providers/hyperliquid` | DataProviderHistorical | Crypto perpetuals: candles and funding/context |
| tradier | `brokerages/tradier` | Brokerage | Order routing plus history and live quote polling (US equities/options) |
| fred | `custom_data/fred` | CustomData | FRED economic series (full-history source) |
| cboe_vix | `custom_data/cboe_vix` | CustomData | CBOE VIX daily OHLC |

The `brokerages/` directory also holds brokerage-*model* crates — `alpaca`, `binance`, `coinbase`, `fxcm`, `interactive_brokers`, `oanda`. These implement fee/margin/leverage models for their venue but do not export the plugin macro yet, so they are not installable plugins on their own; they are meant to be composed into a full brokerage plugin (as `tradier` does).

## License

Apache-2.0.
