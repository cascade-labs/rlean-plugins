//! Hyperliquid S3 archive historical data provider.
//!
//! The provider reads optional compressed Hyperliquid archive cache files or
//! downloads the same S3 objects in memory, converts source records into rlean
//! tick parquet partitions, and builds requested bar resolutions from those
//! raw ticks.

pub mod archive;
pub mod history_provider;

pub use archive::{ArchiveBuckets, ArchiveCredentials, S3ArchiveClient};
pub use history_provider::{HyperliquidArchiveConfig, HyperliquidHistoryProvider};

use lean_data_providers::IHistoryProvider;
use lean_plugin::{rlean_plugin, PluginKind};
use std::collections::HashMap;
use std::ffi::CStr;
use std::sync::Arc;

rlean_plugin! {
    name    = "hyperliquid",
    version = "0.1.0",
    kind    = PluginKind::DataProviderHistorical,
}

/// C-stable factory: create a HyperliquidHistoryProvider from JSON config.
///
/// # Safety
///
/// `config_json` must be a valid null-terminated UTF-8 C string.
/// The returned pointer is a heap-allocated `Box<Arc<dyn IHistoryProvider>>`
/// cast to `*mut ()`. The caller must free it with
/// `rlean_destroy_history_provider`.
#[no_mangle]
pub unsafe extern "C" fn rlean_create_history_provider(
    config_json: *const std::os::raw::c_char,
) -> *mut () {
    let json = unsafe { CStr::from_ptr(config_json) }
        .to_str()
        .unwrap_or("{}");
    let config: serde_json::Value = serde_json::from_str(json).unwrap_or_default();

    let Some(data_root) = config["data_root"].as_str() else {
        eprintln!("rlean-plugin-hyperliquid: framework did not provide data_root.");
        return std::ptr::null_mut();
    };

    let data_root = std::path::PathBuf::from(data_root);
    let cache_dir = config["cache_dir"]
        .as_str()
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::var("HYPERLIQUID_ARCHIVE_CACHE_DIR")
                .ok()
                .map(Into::into)
        });

    let market_bucket = config["market_bucket"]
        .as_str()
        .map(str::to_string)
        .or_else(|| std::env::var("HYPERLIQUID_MARKET_BUCKET").ok())
        .unwrap_or_else(|| "hyperliquid-archive".to_string());
    let fills_bucket = config["fills_bucket"]
        .as_str()
        .map(str::to_string)
        .or_else(|| std::env::var("HYPERLIQUID_FILLS_BUCKET").ok())
        .unwrap_or_else(|| "hl-mainnet-node-data".to_string());
    let request_payer = config["request_payer"]
        .as_str()
        .map(str::to_string)
        .or_else(|| std::env::var("HYPERLIQUID_REQUEST_PAYER").ok())
        .unwrap_or_else(|| "requester".to_string());
    let region = config_string(&config, "aws_region")
        .or_else(|| config_string(&config, "AWS_REGION"))
        .or_else(|| std::env::var("AWS_REGION").ok())
        .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok())
        .unwrap_or_else(default_archive_region);
    let credentials = parse_archive_credentials(&config);

    let coin_map = parse_coin_map(&config["coin_map"]);

    let archive = S3ArchiveClient::new(
        cache_dir,
        ArchiveBuckets {
            market: market_bucket,
            fills: fills_bucket,
        },
        request_payer,
        region,
        credentials,
    );
    let provider = Arc::new(HyperliquidHistoryProvider::new(
        &data_root,
        archive,
        HyperliquidArchiveConfig { coin_map },
    ));
    let boxed: Box<Arc<dyn IHistoryProvider>> = Box::new(provider);
    Box::into_raw(boxed) as *mut ()
}

fn parse_archive_credentials(config: &serde_json::Value) -> Option<ArchiveCredentials> {
    let access_key_id = config_string(config, "aws_access_key_id")
        .or_else(|| config_string(config, "AWS_ACCESS_KEY_ID"))
        .or_else(|| std::env::var("AWS_ACCESS_KEY_ID").ok())?;
    let secret_access_key = config_string(config, "aws_secret_access_key")
        .or_else(|| config_string(config, "AWS_SECRET_ACCESS_KEY"))
        .or_else(|| std::env::var("AWS_SECRET_ACCESS_KEY").ok())?;
    let session_token = config_string(config, "aws_session_token")
        .or_else(|| config_string(config, "AWS_SESSION_TOKEN"))
        .or_else(|| std::env::var("AWS_SESSION_TOKEN").ok());

    Some(ArchiveCredentials {
        access_key_id,
        secret_access_key,
        session_token,
    })
}

fn config_string(config: &serde_json::Value, key: &str) -> Option<String> {
    config[key]
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn default_archive_region() -> String {
    "ap-northeast-1".to_string()
}

fn parse_coin_map(value: &serde_json::Value) -> HashMap<String, String> {
    value
        .as_object()
        .map(|object| {
            object
                .iter()
                .filter_map(|(key, value)| {
                    value
                        .as_str()
                        .map(|coin| (normalise_symbol_key(key), coin.to_ascii_uppercase()))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn normalise_symbol_key(value: &str) -> String {
    value.trim().to_ascii_uppercase()
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
