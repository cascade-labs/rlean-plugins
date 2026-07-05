//! Live integration check for the dividend price-factor fix.
//!
//! Ignored by default (needs network + MASSIVE_API_KEY). Run with:
//!   MASSIVE_API_KEY=... cargo test -p rlean-plugin-massive --test live_factor_file -- --ignored --nocapture

use lean_core::{Market, Symbol};
use lean_data_providers::IHistoryProvider;
use rlean_plugin_massive::MassiveHistoryProvider;

#[test]
#[ignore]
fn key_factor_file_has_dividend_price_factors() {
    // The Massive client uses a blocking reqwest client + thread sleeps, which
    // cannot be driven from inside a tokio runtime. Use a non-tokio executor.
    let api_key = std::env::var("MASSIVE_API_KEY").expect("set MASSIVE_API_KEY");
    let provider = MassiveHistoryProvider::new(api_key, ".", 5.0);
    let symbol = Symbol::create_equity("KEY", &Market::usa());
    let rows = futures::executor::block_on(provider.get_factor_file(&symbol))
        .expect("factor file fetch should succeed");

    println!("KEY factor rows: {}", rows.len());
    for r in &rows {
        println!(
            "  {} price_factor={} split_factor={} ref={}",
            r.date, r.price_factor, r.split_factor, r.reference_price
        );
    }

    let nonunit = rows.iter().filter(|r| (r.price_factor - 1.0).abs() > 1e-9).count();
    assert!(
        nonunit > 0,
        "KEY pays quarterly dividends; expected at least one non-unit price_factor row, got {nonunit}"
    );
}
