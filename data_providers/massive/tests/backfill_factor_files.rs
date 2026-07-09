//! One-off backfill for issue #27: refetch factor files for the 22 tickers that
//! have ZERO rows in the shared `factor_files` Iceberg table and APPEND them
//! (append-only — never resets/compacts).
//!
//! Run against the shared warehouse:
//!   MASSIVE_API_KEY=... RLEAN_DATA=/Volumes/data_cache/rlean/iceberg \
//!     cargo test -p rlean-plugin-massive --test backfill_factor_files \
//!     -- --ignored --nocapture
//!
//! Idempotent: skips any ticker that already has rows so a re-run cannot
//! duplicate. Ignored by default (needs network + MASSIVE_API_KEY + warehouse).

use lean_core::{Market, Symbol};
use lean_data_providers::IHistoryProvider;
use lean_storage::IcebergStore;
use rlean_plugin_massive::MassiveHistoryProvider;

const AFFECTED: &[&str] = &[
    "ACRS", "ARRY", "BXP", "CIM", "CLAR", "DPST", "EOSE", "EVEX", "FINV", "GGAL", "IEP", "MANU",
    "MIST", "MREO", "SIGA", "SMCI", "SOUN", "TRVN", "TSLL", "VKTX", "VLY", "WAL",
];

#[test]
#[ignore]
fn backfill_missing_factor_files() {
    let api_key = std::env::var("MASSIVE_API_KEY").expect("set MASSIVE_API_KEY");
    let data_root = std::env::var("RLEAN_DATA")
        .expect("set RLEAN_DATA to the warehouse data root (parent of iceberg/)");
    let provider = MassiveHistoryProvider::new(api_key, ".", 5.0);
    let market = Market::usa();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let store = rt
        .block_on(IcebergStore::connect_local(&data_root))
        .expect("connect warehouse");

    let mut appended = 0usize;
    for &t in AFFECTED {
        let symbol = Symbol::create_equity(t, &market);

        // Skip if rows already exist (idempotent re-run guard).
        let existing = rt
            .block_on(store.scan_factor_file("usa", &t.to_lowercase()))
            .unwrap_or_default();
        if !existing.is_empty() {
            println!("{t}: already has {} rows, skipping", existing.len());
            continue;
        }

        // Fetch on a non-tokio executor (blocking reqwest client).
        let rows = match futures::executor::block_on(provider.get_factor_file(&symbol)) {
            Ok(rows) if !rows.is_empty() => rows,
            Ok(_) => {
                println!("{t}: provider returned EMPTY — not appending");
                continue;
            }
            Err(e) => {
                println!("{t}: fetch ERROR {e} — not appending");
                continue;
            }
        };
        let nonunit = rows
            .iter()
            .filter(|r| (r.price_factor - 1.0).abs() > 1e-9 || (r.split_factor - 1.0).abs() > 1e-9)
            .count();

        rt.block_on(store.append_factor_files(&[(
            "usa".to_string(),
            t.to_lowercase(),
            rows.clone(),
        )]))
        .expect("append factor rows");
        appended += 1;
        println!("{t}: appended {} rows ({nonunit} non-unit)", rows.len());
    }
    println!("backfill complete: {appended} tickers appended");
}
