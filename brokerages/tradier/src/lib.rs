pub mod brokerage;
pub mod brokerage_model;
pub mod client;
pub mod live_provider;
pub mod models;

pub use brokerage::TradierBrokerage;
pub use brokerage_model::TradierBrokerageModel;
pub use client::TradierClient;
pub use live_provider::{TradierLiveConfig, TradierLiveDataProvider};

use lean_brokerages::Brokerage;
use lean_data::DataQueueHandler;
use lean_plugin::{rlean_plugin, PluginKind};
use std::ffi::CStr;

rlean_plugin! {
    name    = "tradier",
    version = "0.1.0",
    kind    = PluginKind::Brokerage,
}

#[no_mangle]
pub unsafe extern "C" fn rlean_create_brokerage(
    config_json: *const std::os::raw::c_char,
) -> *mut () {
    let json = unsafe { CStr::from_ptr(config_json) }
        .to_str()
        .unwrap_or("{}");
    let config: serde_json::Value = serde_json::from_str(json).unwrap_or_default();

    let Some(access_token) = config_string(&config, "access_token")
        .or_else(|| config_string(&config, "tradier_access_token"))
        .or_else(|| config_string(&config, "tradier-access-token"))
        .or_else(|| std::env::var("TRADIER_ACCESS_TOKEN").ok())
    else {
        eprintln!("rlean-plugin-tradier: missing access_token.");
        return std::ptr::null_mut();
    };

    let Some(account_id) = config_string(&config, "account_id")
        .or_else(|| config_string(&config, "tradier_account_id"))
        .or_else(|| config_string(&config, "tradier-account-id"))
        .or_else(|| std::env::var("TRADIER_ACCOUNT_ID").ok())
    else {
        eprintln!("rlean-plugin-tradier: missing account_id.");
        return std::ptr::null_mut();
    };

    let brokerage: Box<dyn Brokerage> = Box::new(TradierBrokerage::new(
        access_token,
        account_id,
        parse_sandbox(&config),
    ));
    Box::into_raw(Box::new(brokerage)) as *mut ()
}

#[no_mangle]
pub unsafe extern "C" fn rlean_destroy_brokerage(ptr: *mut ()) {
    if !ptr.is_null() {
        drop(unsafe { Box::from_raw(ptr as *mut Box<dyn Brokerage>) });
    }
}

#[no_mangle]
pub unsafe extern "C" fn rlean_create_live_data_provider(
    config_json: *const std::os::raw::c_char,
) -> *mut () {
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
pub unsafe extern "C" fn rlean_destroy_live_data_provider(ptr: *mut ()) {
    if !ptr.is_null() {
        drop(unsafe { Box::from_raw(ptr as *mut Box<dyn DataQueueHandler>) });
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
