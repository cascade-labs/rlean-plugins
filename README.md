# rlean-plugins

Runtime plugins for [rlean](../rlean/) — brokerages, data providers, and custom data sources. Each plugin is a Rust `cdylib` crate compiled independently and loaded from `~/.rlean/plugins/` at startup.

## Plugin Kinds

| Kind | Directory | Trait | Factory export |
|---|---|---|---|
| Data provider | `data_providers/<name>/` | `IHistoryProvider` | `rlean_create_history_provider` / `rlean_destroy_history_provider` |
| Brokerage | `brokerages/<name>/` | `BrokerageModel` + `Brokerage` | see brokerage section |
| Custom data | `custom_data/<name>/` | `ICustomDataSource` | `rlean_custom_data_factory` |

---

## Creating a Plugin

### 1. Create the crate

Add a new directory under the appropriate subdirectory:

```
data_providers/myprovider/
brokerages/mybroker/
custom_data/mysource/
```

`Cargo.toml` — follow the naming convention and use workspace deps:

```toml
[package]
name = "rlean-plugin-myprovider"
version.workspace = true
edition.workspace = true
description = "rlean plugin: MyProvider historical data"

[lib]
name = "rlean_plugin_myprovider"   # underscore name used for the .dylib filename
crate-type = ["cdylib", "rlib"]    # cdylib for the loader, rlib for tests/dev

[dependencies]
lean-core           = { workspace = true }
lean-plugin         = { workspace = true }
lean-data-providers = { workspace = true }  # data providers
lean-brokerages     = { workspace = true }  # brokerages
# lean-data, lean-storage, etc. as needed
anyhow      = { workspace = true }
serde       = { workspace = true }
serde_json  = { workspace = true }
```

The workspace `Cargo.toml` already declares all common deps — pull from there rather than specifying versions directly.

### 2. Export the plugin descriptor

Every plugin must call the `rlean_plugin!` macro in `src/lib.rs`:

```rust
use lean_plugin::{PluginKind, rlean_plugin};

rlean_plugin! {
    name    = "myprovider",
    version = "0.1.0",
    kind    = PluginKind::DataProviderHistorical,  // or Brokerage / CustomData
}
```

This expands to a `#[no_mangle] pub extern "C" fn rlean_plugin_descriptor()` that the loader calls first.

---

## Data Provider Plugin

Implement `IHistoryProvider` from `lean-data-providers`, then export two C-stable factory functions.

```rust
use lean_data_providers::IHistoryProvider;
use lean_plugin::{PluginKind, rlean_plugin};
use std::ffi::CStr;
use std::sync::Arc;

pub struct MyProvider { /* ... */ }

impl IHistoryProvider for MyProvider {
    // implement history request methods
}

rlean_plugin! {
    name    = "myprovider",
    version = "0.1.0",
    kind    = PluginKind::DataProviderHistorical,
}

/// Create a provider from a JSON config string.
/// Config keys are set via `rlean config set myprovider.<key> <value>`.
///
/// # Safety
/// `config_json` must be a valid null-terminated UTF-8 C string.
/// Returns a heap-allocated `Box<Arc<dyn IHistoryProvider>>` cast to `*mut ()`.
/// Free with `rlean_destroy_history_provider`.
#[no_mangle]
pub unsafe extern "C" fn rlean_create_history_provider(
    config_json: *const std::os::raw::c_char,
) -> *mut () {
    let json = unsafe { CStr::from_ptr(config_json) }.to_str().unwrap_or("{}");
    let config: serde_json::Value = serde_json::from_str(json).unwrap_or_default();

    let api_key = config["api_key"].as_str().map(|s| s.to_string())
        .or_else(|| std::env::var("MYPROVIDER_API_KEY").ok());

    let provider = Arc::new(MyProvider::new(api_key));
    let boxed: Box<Arc<dyn IHistoryProvider>> = Box::new(provider);
    Box::into_raw(boxed) as *mut ()
}

/// Free a provider returned by `rlean_create_history_provider`.
///
/// # Safety
/// `ptr` must have been returned by `rlean_create_history_provider` and not yet freed.
#[no_mangle]
pub unsafe extern "C" fn rlean_destroy_history_provider(ptr: *mut ()) {
    if !ptr.is_null() {
        drop(unsafe { Box::from_raw(ptr as *mut Arc<dyn IHistoryProvider>) });
    }
}
```

See `data_providers/thetadata/` for a complete implementation.

### Data storage

All data fetched must be written as Parquet via `lean-storage`. **Never write CSV.** See the [data architecture section](#data-architecture) below.

---

## Custom Data Plugin

Implement `ICustomDataSource` from `lean-data-providers`, then export `rlean_custom_data_factory`.

```rust
use lean_data::custom::{CustomDataConfig, CustomDataPoint, CustomDataSource, ICustomDataSource};
use lean_plugin::{PluginKind, rlean_plugin};
use chrono::NaiveDate;

pub struct MyDataSource;

impl ICustomDataSource for MyDataSource {
    fn name(&self) -> &str { "mysource" }

    /// Return the URL/path to fetch data for `ticker` on `date`.
    fn get_source(
        &self,
        ticker: &str,
        date: NaiveDate,
        config: &CustomDataConfig,
    ) -> Option<CustomDataSource> {
        // build and return a CustomDataSource { uri, transport, format }
        todo!()
    }

    /// Parse a single line from the fetched data into a CustomDataPoint.
    /// Return None to skip the line (header, wrong date, missing value, etc.).
    fn reader(
        &self,
        line: &str,
        date: NaiveDate,
        config: &CustomDataConfig,
    ) -> Option<CustomDataPoint> {
        todo!()
    }
}

rlean_plugin! {
    name    = "mysource",
    version = "0.1.0",
    kind    = PluginKind::CustomData,
}

/// C-stable factory. Returns a double-boxed `dyn ICustomDataSource` as `*mut ()`.
///
/// Double-boxing is required because fat pointers (`*mut dyn Trait`) are not
/// C-ABI-safe — only thin (8-byte) pointers can cross the FFI boundary reliably.
///
/// The loader frees this via: `*Box::from_raw(raw as *mut Box<dyn ICustomDataSource>)`
#[no_mangle]
pub extern "C" fn rlean_custom_data_factory() -> *mut () {
    let source: Box<dyn ICustomDataSource> = Box::new(MyDataSource);
    Box::into_raw(Box::new(source)) as *mut ()
}
```

If the source serves the entire series history in one download (e.g., FRED), override:

```rust
fn is_full_history_source(&self) -> bool { true }

fn read_history_line(&self, line: &str, config: &CustomDataConfig) -> Option<CustomDataPoint> {
    // same as reader() but without date filtering — date comes from the line itself
    todo!()
}
```

See `custom_data/fred/` and `custom_data/cboe_vix/` for complete implementations.

---

## Brokerage Plugin

Implement `BrokerageModel` (backtesting fee/leverage model) and optionally `Brokerage` (live order routing).

```rust
use lean_brokerages::{Brokerage, BrokerageModel};
use lean_orders::security_transaction_model::{OrderFee, OrderFeeParameters, SecurityTransactionModel};

pub struct MyFeeModel;

impl SecurityTransactionModel for MyFeeModel {
    fn get_order_fee(&self, params: &OrderFeeParameters) -> OrderFee {
        // e.g. $0 commission
        OrderFee::zero()
    }
}

pub struct MyBrokerageModel;

impl BrokerageModel for MyBrokerageModel {
    fn name(&self) -> &str { "MyBroker" }
    fn transaction_model(&self) -> Box<dyn SecurityTransactionModel> { Box::new(MyFeeModel) }
    fn default_leverage(&self) -> f64 { 2.0 }
    fn can_submit_order(&self) -> bool { true }
    fn can_update_order(&self) -> bool { true }
    fn can_execute_order(&self) -> bool { true }
}
```

For live trading, also implement `Brokerage` (connect/disconnect, place/cancel/update order, fetch positions). See `brokerages/tradier/` for a complete example.

---

## Data Architecture

**All data is Parquet. No CSV.**

- Use `lean-storage` types (`ParquetWriter`, `ParquetReader`) for all persistence.
- If an upstream API returns CSV (e.g., FRED's `fredgraph.csv`), parse it in memory and write Parquet — never persist the raw CSV.
- Do not add `csv` or `serde_csv` as dependencies.

---

## Register the Plugin

Add an entry to `registry.json` so `rlean plugin list` / `rlean plugin install` can find it:

```json
{
  "name": "myprovider",
  "version": "0.1.0",
  "kind": "data-provider",
  "description": "MyProvider historical data",
  "git_url": "https://github.com/cascade-labs/rlean-plugins",
  "subdir": "data_providers/myprovider"
}
```

`kind` must be one of: `data-provider`, `brokerage`, `custom-data`.

---

## Local Development

rlean crates are fetched from GitHub by default. To point at a local checkout instead, create a `.cargo/config.toml` in this repo root (it is gitignored):

```toml
[patch."ssh://git@github.com/cascade-labs/rlean"]
lean-core           = { path = "../rlean/crates/lean-core" }
lean-data           = { path = "../rlean/crates/lean-data" }
lean-storage        = { path = "../rlean/crates/lean-storage" }
lean-data-providers = { path = "../rlean/crates/lean-data-providers" }
lean-plugin         = { path = "../rlean/crates/lean-plugin" }
lean-brokerages     = { path = "../rlean/crates/lean-brokerages" }
lean-orders         = { path = "../rlean/crates/lean-orders" }
```

The rlean source is at `../rlean/` relative to this repo.

---

## Build and Install

```sh
# Build a single plugin
cargo build --release -p rlean-plugin-myprovider

# Build all plugins
cargo build --release

# Install manually (rlean plugin install does this automatically)
cp target/release/librlean_plugin_myprovider.dylib ~/.rlean/plugins/
```
