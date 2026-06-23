use anyhow::{bail, Result};

pub const LIVE_BASE: &str = "https://api.tradier.com/v1";
pub const SANDBOX_BASE: &str = "https://sandbox.tradier.com/v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradierEnvironment {
    Live,
    Paper,
}

impl TradierEnvironment {
    pub fn is_sandbox(self) -> bool {
        matches!(self, Self::Paper)
    }

    pub fn base_url(self) -> &'static str {
        match self {
            Self::Live => LIVE_BASE,
            Self::Paper => SANDBOX_BASE,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Paper => "paper",
        }
    }
}

pub fn config_string(config: &serde_json::Value, key: &str) -> Option<String> {
    config[key]
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

pub fn access_token_from_config(config: &serde_json::Value) -> Option<String> {
    config_string(config, "access_token")
        .or_else(|| config_string(config, "tradier_access_token"))
        .or_else(|| config_string(config, "tradier-access-token"))
}

pub fn account_id_from_config(config: &serde_json::Value) -> Option<String> {
    config_string(config, "account_id")
        .or_else(|| config_string(config, "tradier_account_id"))
        .or_else(|| config_string(config, "tradier-account-id"))
}

pub fn trading_environment_from_config(config: &serde_json::Value) -> Result<TradierEnvironment> {
    if let Some(value) = first_config_string(
        config,
        &["environment", "tradier_environment", "tradier-environment"],
    ) {
        return parse_environment(&value, "environment");
    }
    if let Some(value) = first_config_bool(
        config,
        &[
            "use_sandbox",
            "sandbox",
            "tradier_use_sandbox",
            "tradier-use-sandbox",
        ],
    ) {
        return Ok(if value {
            TradierEnvironment::Paper
        } else {
            TradierEnvironment::Live
        });
    }
    Ok(TradierEnvironment::Live)
}

pub fn market_data_environment_from_config(
    config: &serde_json::Value,
) -> Result<TradierEnvironment> {
    trading_environment_from_config(config)
}

fn first_config_string(config: &serde_json::Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| config_string(config, key))
}

fn first_config_bool(config: &serde_json::Value, keys: &[&str]) -> Option<bool> {
    keys.iter().find_map(|key| {
        config[key]
            .as_bool()
            .or_else(|| config_bool_string(config, key))
    })
}

fn config_bool_string(config: &serde_json::Value, key: &str) -> Option<bool> {
    parse_bool(config[key].as_str()?)
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "y" => Some(true),
        "false" | "0" | "no" | "n" => Some(false),
        _ => None,
    }
}

fn parse_environment(value: &str, context: &str) -> Result<TradierEnvironment> {
    match value.trim().to_ascii_lowercase().as_str() {
        "live" | "prod" | "production" | "real" => Ok(TradierEnvironment::Live),
        "paper" | "sandbox" | "test" => Ok(TradierEnvironment::Paper),
        other => bail!("invalid Tradier {context}: {other}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn environment_paper_selects_sandbox_for_trading() {
        let config = json!({ "environment": "paper" });
        assert_eq!(
            trading_environment_from_config(&config).unwrap(),
            TradierEnvironment::Paper
        );
    }

    #[test]
    fn csharp_hyphenated_environment_alias_is_supported() {
        let config = json!({ "tradier-environment": "paper" });
        assert_eq!(
            trading_environment_from_config(&config).unwrap(),
            TradierEnvironment::Paper
        );
    }

    #[test]
    fn market_data_uses_paper_environment_for_rest_requests() {
        let config = json!({ "environment": "paper" });
        assert_eq!(
            market_data_environment_from_config(&config).unwrap(),
            TradierEnvironment::Paper
        );
    }

    #[test]
    fn tradier_use_sandbox_alias_selects_sandbox_for_trading() {
        let config = json!({ "tradier-use-sandbox": true });
        assert_eq!(
            trading_environment_from_config(&config).unwrap(),
            TradierEnvironment::Paper
        );
    }
}
