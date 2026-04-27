pub mod client;
pub mod corporate_actions;
pub mod history_provider;
pub mod models;

pub use client::MassiveRestClient;
pub use corporate_actions::{
    compute_map_file_rows, factor_for_date, fetch_and_write_factor_file, fetch_and_write_map_file,
    read_factor_file, write_factor_file, write_map_file,
};
pub use history_provider::MassiveHistoryProvider;

use lean_data_providers::IHistoryProvider;
use lean_plugin::{rlean_plugin, PluginKind};
use std::ffi::CStr;
use std::sync::Arc;

rlean_plugin! {
    name    = "massive",
    version = "0.1.0",
    kind    = PluginKind::DataProviderHistorical,
}

/// C-stable factory: create a MassiveHistoryProvider from a JSON config string.
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

    // API key is required.  Priority: JSON config → MASSIVE_API_KEY env var.
    let api_key = config["api_key"]
        .as_str()
        .map(|s| s.to_string())
        .or_else(|| std::env::var("MASSIVE_API_KEY").ok())
        .unwrap_or_default();
    if api_key.is_empty() {
        eprintln!(
            "rlean-plugin-massive: MASSIVE_API_KEY is not set. \
                   Pass api_key in config or set the MASSIVE_API_KEY environment variable."
        );
        return std::ptr::null_mut();
    }
    let data_root = std::path::PathBuf::from(config["data_root"].as_str().unwrap_or("data"));
    let rps = config["requests_per_second"].as_f64().unwrap_or(5.0);

    let provider = Arc::new(MassiveHistoryProvider::new(api_key, &data_root, rps));
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
