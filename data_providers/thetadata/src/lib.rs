//! ThetaData REST API client and historical data provider — rlean plugin.
//!
//! Exports the plugin ABI (`rlean_plugin_descriptor`, `rlean_create_history_provider`,
//! `rlean_destroy_history_provider`) as well as the public Rust API for use when
//! this crate is linked as an `rlib` during development.
pub mod client;
pub mod history_provider;
pub mod models;

pub use client::ThetaDataClient;
pub use history_provider::ThetaDataHistoryProvider;

use lean_data_providers::IHistoryProvider;
use lean_plugin::{PluginKind, rlean_plugin};
use std::sync::Arc;
use std::ffi::CStr;

rlean_plugin! {
    name    = "thetadata",
    version = "0.1.0",
    kind    = PluginKind::DataProviderHistorical,
}

/// C-stable factory: create a ThetaDataHistoryProvider from a JSON config string.
///
/// # Safety
///
/// `config_json` must be a valid null-terminated UTF-8 C string.
/// The returned pointer is a heap-allocated `Box<Arc<dyn IHistoryProvider>>`
/// cast to `*mut ()`.  The caller must free it with `rlean_destroy_history_provider`.
#[no_mangle]
pub unsafe extern "C" fn rlean_create_history_provider(
    config_json: *const std::os::raw::c_char,
) -> *mut () {
    let json = unsafe { CStr::from_ptr(config_json) }
        .to_str()
        .unwrap_or("{}");
    let config: serde_json::Value = serde_json::from_str(json).unwrap_or_default();

    // Config priority: JSON config → env var → default.
    // Bearer token is optional (not needed for local sidecar).
    let api_key = config["api_key"].as_str().map(|s| s.to_string())
        .or_else(|| std::env::var("THETADATA_API_KEY").ok());
    let base_url = config["base_url"].as_str().map(|s| s.to_string())
        .or_else(|| std::env::var("THETADATA_BASE_URL").ok());
    let data_root = std::path::PathBuf::from(
        config["data_root"].as_str().unwrap_or("data")
    );
    let rps = config["requests_per_second"].as_f64().unwrap_or(4.0);
    let max_concurrent = config["max_concurrent"].as_u64().unwrap_or(4) as usize;

    let provider = Arc::new(ThetaDataHistoryProvider::new(
        api_key,
        base_url,
        &data_root,
        rps,
        max_concurrent,
    ));
    let boxed: Box<Arc<dyn IHistoryProvider>> = Box::new(provider);
    Box::into_raw(boxed) as *mut ()
}

/// Free a provider returned by `rlean_create_history_provider`.
///
/// # Safety
///
/// `ptr` must have been returned by `rlean_create_history_provider` and must
/// not have been freed already.
#[no_mangle]
pub unsafe extern "C" fn rlean_destroy_history_provider(ptr: *mut ()) {
    if !ptr.is_null() {
        drop(unsafe { Box::from_raw(ptr as *mut Arc<dyn IHistoryProvider>) });
    }
}
