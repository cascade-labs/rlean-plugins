# rlean-plugins — Claude Project Context

Runtime plugins for rlean: brokerages, data providers, and custom data sources. Each plugin is a Rust `cdylib` crate compiled independently and loaded from `~/.rlean/plugins/` at runtime via the `lean-plugin` ABI.

## Workspace Layout

```
brokerages/
  alpaca/            # Commission-free US equities
  binance/           # Spot + USDT futures
  coinbase/
  fidelity/
  fxcm/
  interactive_brokers/
  oanda/
  robinhood/
  tradier/           # Options-focused brokerage (live trading tested)

data_providers/
  massive/           # Massive.com (formerly Polygon.io) historical data
  thetadata/         # ThetaData — options EOD chains + equity history

custom_data/
  cboe_vix/          # CBOE VIX index data
  fred/              # FRED macroeconomic data series

registry.json        # Plugin registry manifest (name, kind, git URL, description)
```

## Plugin ABI

Every plugin exports a descriptor and one or more factory functions:

```rust
use lean_plugin::{PluginDescriptor, PluginKind};

#[no_mangle]
pub extern "C" fn rlean_plugin_descriptor() -> PluginDescriptor {
    PluginDescriptor {
        name:    c"myprovider",
        version: c"0.1.0",
        kind:    PluginKind::DataProvider,   // or PluginKind::Brokerage / CustomData
    }
}
```

- **Data providers** implement `IHistoryProvider` — return historical bars/chains on request.
- **Brokerages** implement `IBrokerageModel` — provide fill models, fee schedules, margin rules.
- **Custom data** implement the custom data source trait — fetch and parse non-standard data.

## Data Architecture — Parquet Only

**No CSV anywhere in this repo. This is absolute and non-negotiable.**

- All data fetched by plugins must be written as Parquet using `lean-storage` types.
- Never add CSV reading, CSV writing, or `csv`/`serde_csv` dependencies to any plugin.
- If a source API returns CSV (e.g., FRED), parse it in memory and write Parquet — do not persist the CSV.
- `lean_csv_reader.rs` and `parquet_migration.rs` do not exist and must not be recreated.

## ThetaData Plugin Notes

- Option chain requests require `strike=*` and `expiration=*` to get full chain.
- API `symbol` field returns only the root ticker (e.g., `SPY`), not the full OCC symbol.
- Store option EOD chains at: `option/usa/daily/{ticker}_eod.parquet`
- Auth: Bearer token (`AzMCXMQmZJ0vkxsCYr0Wqe_HkMFIMdlN4TbqLJTEAm8`) via `thetadata.api_key` config key.

## Massive (Polygon) Plugin Notes

- API key stored at `massive.api_key` config key.
- Used for equity factor file and map file generation (coarse universe support).

## registry.json

Lists all plugins available via `rlean plugin list` / `rlean plugin install`. Each entry needs:
- `name` — CLI name (e.g., `thetadata`)
- `kind` — `data-provider`, `brokerage`, or `custom-data`
- `description` — shown in `rlean plugin list`
- `git` — canonical source URL

## Build

Each plugin is its own `cdylib`:

```sh
cargo build --release -p thetadata
cargo build --release -p alpaca
cargo build --release             # all plugins
```

The output `.dylib`/`.so` is copied to `~/.rlean/plugins/` by `rlean plugin install`.
