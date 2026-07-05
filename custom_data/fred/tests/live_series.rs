//! Live integration: FRED full-history CSV via get_source + read_history_line.

use chrono::NaiveDate;
use lean_data::custom::CustomDataConfig;
use lean_data_providers::ICustomDataSource;
use rlean_plugin_fred::FredDataSource;
use std::collections::HashMap;

#[test]
fn live_fred_dfii10_downloads_and_parses() {
    let src = FredDataSource::new();
    let day = NaiveDate::from_ymd_opt(2019, 1, 2).unwrap();
    let config = CustomDataConfig {
        ticker: "DFII10".to_string(),
        source_type: "fred".to_string(),
        resolution: lean_core::Resolution::Daily,
        properties: HashMap::new(),
        query: Default::default(),
    };
    let data_source = src
        .get_source("DFII10", day, &config)
        .expect("get_source returned None");
    eprintln!("fred url: {}", data_source.uri);

    let client = reqwest::blocking::Client::builder()
        .user_agent("rlean-live-test/0.1")
        .http1_only()
        .timeout(std::time::Duration::from_secs(120))
        .build()
        .unwrap();
    let mut body = String::new();
    let mut last_err = None;
    for attempt in 1..=3 {
        match client.get(&data_source.uri).send() {
            Ok(resp) => match resp.error_for_status() {
                Ok(resp) => match resp.text() {
                    Ok(text) if text.len() > 100 => {
                        body = text;
                        break;
                    }
                    Ok(_) => last_err = Some("empty body".to_string()),
                    Err(e) => last_err = Some(e.to_string()),
                },
                Err(e) => last_err = Some(e.to_string()),
            },
            Err(e) => last_err = Some(e.to_string()),
        }
        eprintln!("fred attempt {attempt} failed: {:?}", last_err);
        std::thread::sleep(std::time::Duration::from_secs(2));
    }
    let body = body;
    if body.is_empty() {
        panic!("fred http failed after retries: {:?}", last_err);
    }
    eprintln!("fred csv bytes: {}", body.len());

    let mut parsed = 0usize;
    let mut matched = 0usize;
    for line in body.lines() {
        if src.read_history_line(line, &config).is_some() {
            parsed += 1;
        }
        if let Some(pt) = src.reader(line, day, &config) {
            matched += 1;
            eprintln!("matched {day}: value={}", pt.value);
        }
    }
    eprintln!("read_history_line rows: {parsed}, reader matched {day}: {matched}");
    assert!(parsed > 100, "expected full FRED history parse");
    assert_eq!(matched, 1, "expected exactly one row for {day}");
}
