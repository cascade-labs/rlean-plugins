pub mod brokerage;
pub mod brokerage_model;
pub mod client;
pub mod config;
pub mod history_provider;
pub mod live_provider;
pub mod models;

pub use brokerage::TradierBrokerage;
pub use brokerage_model::TradierBrokerageModel;
pub use client::TradierClient;
pub use history_provider::TradierHistoryProvider;
pub use live_provider::{TradierLiveConfig, TradierLiveDataProvider};

use lean_brokerages::Brokerage;
use lean_data::DataQueueHandler;
use lean_data_providers::IHistoryProvider;
use lean_plugin::{ensure_crypto_provider, rlean_plugin, PluginKind};
use std::ffi::CStr;
use std::sync::Arc;

use config::{
    access_token_from_config, account_id_from_config, market_data_environment_from_config,
    trading_environment_from_config,
};

rlean_plugin! {
    name    = "tradier",
    version = "0.1.0",
    kind    = PluginKind::Brokerage,
}

#[no_mangle]
/// # Safety
///
/// `config_json` must be null or point to a valid, NUL-terminated C string for
/// the duration of the call. The returned pointer must be released with
/// `rlean_destroy_history_provider`.
pub unsafe extern "C" fn rlean_create_history_provider(
    config_json: *const std::os::raw::c_char,
) -> *mut () {
    ensure_crypto_provider();

    let json = unsafe { CStr::from_ptr(config_json) }
        .to_str()
        .unwrap_or("{}");
    let config: serde_json::Value = serde_json::from_str(json).unwrap_or_default();

    let Some(access_token) = access_token_from_config(&config) else {
        eprintln!("rlean-plugin-tradier: missing access_token.");
        return std::ptr::null_mut();
    };
    let market_data_environment = match market_data_environment_from_config(&config) {
        Ok(environment) => environment,
        Err(error) => {
            eprintln!("rlean-plugin-tradier: invalid market data config: {error}");
            return std::ptr::null_mut();
        }
    };

    let provider: Arc<dyn IHistoryProvider> = Arc::new(TradierHistoryProvider::new(
        access_token,
        market_data_environment.is_sandbox(),
    ));
    Box::into_raw(Box::new(provider)) as *mut ()
}

#[no_mangle]
/// # Safety
///
/// `ptr` must be null or a pointer previously returned by
/// `rlean_create_history_provider` that has not already been destroyed.
pub unsafe extern "C" fn rlean_destroy_history_provider(ptr: *mut ()) {
    if !ptr.is_null() {
        drop(unsafe { Box::from_raw(ptr as *mut Arc<dyn IHistoryProvider>) });
    }
}

#[no_mangle]
/// # Safety
///
/// `config_json` must be null or point to a valid, NUL-terminated C string for
/// the duration of the call. The returned pointer must be released with
/// `rlean_destroy_brokerage`.
pub unsafe extern "C" fn rlean_create_brokerage(
    config_json: *const std::os::raw::c_char,
) -> *mut () {
    ensure_crypto_provider();

    let json = unsafe { CStr::from_ptr(config_json) }
        .to_str()
        .unwrap_or("{}");
    let config: serde_json::Value = serde_json::from_str(json).unwrap_or_default();

    let Some(access_token) = access_token_from_config(&config) else {
        eprintln!("rlean-plugin-tradier: missing access_token.");
        return std::ptr::null_mut();
    };

    let Some(account_id) = account_id_from_config(&config) else {
        eprintln!("rlean-plugin-tradier: missing account_id.");
        return std::ptr::null_mut();
    };

    let trading_environment = match trading_environment_from_config(&config) {
        Ok(environment) => environment,
        Err(error) => {
            eprintln!("rlean-plugin-tradier: invalid brokerage config: {error}");
            return std::ptr::null_mut();
        }
    };

    let brokerage: Box<dyn Brokerage> = Box::new(TradierBrokerage::new(
        access_token,
        account_id,
        trading_environment,
    ));
    Box::into_raw(Box::new(brokerage)) as *mut ()
}

#[no_mangle]
/// # Safety
///
/// `ptr` must be null or a pointer previously returned by
/// `rlean_create_brokerage` that has not already been destroyed.
pub unsafe extern "C" fn rlean_destroy_brokerage(ptr: *mut ()) {
    if !ptr.is_null() {
        drop(unsafe { Box::from_raw(ptr as *mut Box<dyn Brokerage>) });
    }
}

#[no_mangle]
/// # Safety
///
/// `config_json` must be null or point to a valid, NUL-terminated C string for
/// the duration of the call. The returned pointer must be released with
/// `rlean_destroy_live_data_provider`.
pub unsafe extern "C" fn rlean_create_live_data_provider(
    config_json: *const std::os::raw::c_char,
) -> *mut () {
    ensure_crypto_provider();

    let json = unsafe { CStr::from_ptr(config_json) }
        .to_str()
        .unwrap_or("{}");
    let config: serde_json::Value = serde_json::from_str(json).unwrap_or_default();
    let live_config = match TradierLiveConfig::from_json(&config) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("rlean-plugin-tradier: failed to create live data provider: {error}");
            return std::ptr::null_mut();
        }
    };
    let provider: Box<dyn DataQueueHandler> = Box::new(TradierLiveDataProvider::new(live_config));
    Box::into_raw(Box::new(provider)) as *mut ()
}

#[no_mangle]
/// # Safety
///
/// `ptr` must be null or a pointer previously returned by
/// `rlean_create_live_data_provider` that has not already been destroyed.
pub unsafe extern "C" fn rlean_destroy_live_data_provider(ptr: *mut ()) {
    if !ptr.is_null() {
        drop(unsafe { Box::from_raw(ptr as *mut Box<dyn DataQueueHandler>) });
    }
}
