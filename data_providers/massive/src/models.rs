use serde::Deserialize;

/// Response from Massive's `/v2/aggs/ticker/.../range/...` endpoint.
#[derive(Debug, Deserialize)]
pub struct AggregatesResponse {
    pub status: String,
    #[serde(default)]
    pub results: Option<Vec<AggBar>>,
    /// Pagination: if present, fetch this URL next.
    pub next_url: Option<String>,
}

/// A single aggregate (OHLCV) bar from Massive.
#[derive(Debug, Deserialize)]
pub struct AggBar {
    #[serde(rename = "o")]
    pub open: f64,
    #[serde(rename = "h")]
    pub high: f64,
    #[serde(rename = "l")]
    pub low: f64,
    #[serde(rename = "c")]
    pub close: f64,
    #[serde(rename = "v")]
    pub volume: f64,
    /// Bar open timestamp in Unix milliseconds.
    #[serde(rename = "t")]
    pub timestamp_ms: i64,
}

/// Generic paginated response from Massive's v3 reference endpoints.
#[derive(Debug, Deserialize)]
pub struct PaginatedResponse<T> {
    pub status: String,
    /// `None` when the response contains no results (Massive omits the field).
    pub results: Option<Vec<T>>,
    pub next_url: Option<String>,
}

/// A single stock split from Massive's `/v3/reference/splits` endpoint.
#[derive(Debug, Deserialize)]
pub struct MassiveSplitItem {
    /// Execution date in "YYYY-MM-DD" format.
    pub execution_date: String,
    /// Number of shares before the split.
    pub split_from: f64,
    /// Number of shares after the split.
    pub split_to: f64,
    pub ticker: String,
}

impl MassiveSplitItem {
    /// Factor to multiply pre-split raw prices by to get adjusted prices.
    /// = split_from / split_to  (e.g. 4:1 split → 0.25)
    pub fn split_factor(&self) -> f64 {
        if self.split_to != 0.0 { self.split_from / self.split_to } else { 1.0 }
    }
}

/// A single dividend from Massive's `/v3/reference/dividends` endpoint.
#[derive(Debug, Deserialize)]
pub struct MassiveDividendItem {
    /// Ex-dividend date in "YYYY-MM-DD" format.
    pub ex_dividend_date: String,
    /// Cash amount per share.
    #[serde(default)]
    pub cash_amount: f64,
    pub ticker: String,
    /// "CD" = cash dividend, "SC" = special cash, "LT"/"ST" = capital gains.
    #[serde(default)]
    pub dividend_type: String,
}

/// Response from Massive's `/v3/reference/tickers/{ticker}` endpoint.
#[derive(Debug, Deserialize)]
pub struct TickerDetailsResponse {
    pub results: Option<TickerDetails>,
}

#[derive(Debug, Deserialize)]
pub struct TickerDetails {
    pub ticker: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub primary_exchange: String,
    /// ISO 8601 date string "YYYY-MM-DD" if delisted.
    pub delisted_utc: Option<String>,
    /// "YYYY-MM-DD" listing date.
    pub list_date: Option<String>,
    /// false if delisted/inactive.
    #[serde(default = "default_true")]
    pub active: bool,
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agg_response_deserializes_typical_payload() {
        let json = r#"{
            "ticker": "AAPL",
            "status": "OK",
            "queryCount": 2,
            "resultsCount": 2,
            "adjusted": false,
            "results": [
                {"o": 178.25, "h": 179.00, "l": 177.50, "c": 178.75, "v": 52341200.0, "t": 1700000000000},
                {"o": 178.75, "h": 180.50, "l": 178.00, "c": 180.10, "v": 48900000.0, "t": 1700086400000}
            ],
            "request_id": "abc123"
        }"#;

        let resp: AggregatesResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.status, "OK");
        let results = resp.results.unwrap();
        assert_eq!(results.len(), 2);
        let bar = &results[0];
        assert_eq!(bar.open, 178.25);
    }

    #[test]
    fn agg_response_handles_empty_results_field() {
        let json = r#"{"status": "OK", "resultsCount": 0}"#;
        let resp: AggregatesResponse = serde_json::from_str(json).unwrap();
        assert!(resp.results.is_none());
    }

    #[test]
    fn split_factor_four_for_one() {
        let split = MassiveSplitItem {
            ticker: "AAPL".into(),
            execution_date: "2020-08-31".into(),
            split_from: 1.0,
            split_to: 4.0,
        };
        assert!((split.split_factor() - 0.25).abs() < 1e-9);
    }

    #[test]
    fn split_factor_returns_one_when_split_to_is_zero() {
        let split = MassiveSplitItem {
            ticker: "ERR".into(),
            execution_date: "2023-01-01".into(),
            split_from: 5.0,
            split_to: 0.0,
        };
        assert_eq!(split.split_factor(), 1.0);
    }
}
