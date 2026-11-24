use crate::{d, policy::Rules, Result};
use perp_arb_core::{instruments::common_lot, Decimal};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Instrument {
    pub symbol: String,
    pub tick: Option<String>,
    pub price_rule: String,
    pub settlement: String,
    pub market_id: Option<u64>,
    pub filters: BTreeMap<String, Value>,
}
pub fn parse(info: &Value, base: &str, spot: bool) -> Result<Instrument> {
    let name = format!("{base}USDT");
    let rows = info["symbols"]
        .as_array()
        .ok_or("missing exchange symbols")?;
    let matches: Vec<_> = rows.iter().filter(|r| r["symbol"] == name).collect();
    if matches.len() != 1 {
        return Err(format!("missing/ambiguous exact USDT instrument {name}"));
    }
    let row = matches[0];
    if row["status"] != "TRADING"
        || row["baseAsset"] != base
        || row["quoteAsset"] != "USDT"
        || (spot && row["isSpotTradingAllowed"] != true)
        || (!spot && (row["contractType"] != "PERPETUAL" || row["marginAsset"] != "USDT"))
    {
        return Err("instrument is not an enabled same-unit USDT market".into());
    }
    let mut filters = BTreeMap::new();
    for filter in row["filters"].as_array().ok_or("missing filters")? {
        let name = filter["filterType"]
            .as_str()
            .ok_or("invalid filter type")?
            .to_owned();
        if filters.insert(name, filter.clone()).is_some() {
            return Err("duplicate filter".into());
        }
    }
    let tick = filters
        .get("PRICE_FILTER")
        .and_then(|f| f["tickSize"].as_str())
        .ok_or("missing tick")?
        .to_owned();
    if d(&tick)? <= Decimal::ZERO {
        return Err("invalid tick".into());
    }
    Ok(Instrument {
        symbol: name,
        tick: Some(tick),
        price_rule: "fixed_tick".into(),
        settlement: "USDT".into(),
        market_id: None,
        filters,
    })
}
pub fn hyperliquid(info: &Value, base: &str) -> Result<Instrument> {
    use perp_arb_market_data::catalog::{resolve_hyperliquid, SymbolMappings};
    let resolved = resolve_hyperliquid(info, base, &SymbolMappings::default())?;
    if resolved.units().is_none() {
        return Err("unverified Hyperliquid base units".into());
    }
    let limits = &resolved.limit;
    Ok(Instrument {
        symbol: resolved.key.symbol().into(),
        tick: None,
        price_rule: format!("{:?}", limits.price_rule()),
        settlement: resolved.settlement,
        market_id: None,
        filters: BTreeMap::from([
            (
                "LOT_SIZE".into(),
                serde_json::json!({"stepSize":limits.lot().to_string(),"minQty":limits.min_size().to_string()}),
            ),
            (
                "MIN_NOTIONAL".into(),
                serde_json::json!({"minNotional":limits.min_notional().ok_or("missing HL minimum")?.to_string()}),
            ),
        ]),
    })
}
pub fn lighter(info: &Value, base: &str) -> Result<Instrument> {
    let rows = info["order_book_details"]
        .as_array()
        .ok_or("missing Lighter markets")?;
    let matches: Vec<_> = rows
        .iter()
        .filter(|r| r["symbol"] == base && r["market_type"] == "perp")
        .collect();
    if matches.len() != 1 {
        return Err("missing/ambiguous exact Lighter perpetual".into());
    }
    let row = matches[0];
    if row["status"] != "active"
        || row["is_frozen"] != false
        || row["market_config"]["force_reduce_only"] != false
        || row["quote_asset_id"].as_u64() != Some(0)
        || d(row["multiplier"]
            .as_str()
            .ok_or("missing Lighter multiplier")?)?
            != Decimal::ONE
    {
        return Err("unsupported/inactive Lighter market or unit/collateral".into());
    }
    let precision = |key: &str| -> Result<Decimal> {
        let n = row[key]
            .as_u64()
            .filter(|n| *n <= 18)
            .ok_or("invalid Lighter precision")?;
        Ok(Decimal::new(1, n as u32))
    };
    let step = precision("supported_size_decimals")?;
    let tick = precision("supported_price_decimals")?;
    let positive = |key: &str| -> Result<String> {
        let s = row[key]
            .as_str()
            .ok_or("missing Lighter size/notional constraint")?;
        if d(s)? <= Decimal::ZERO {
            return Err("invalid Lighter constraint".into());
        }
        Ok(s.into())
    };
    Ok(Instrument {
        symbol: base.into(),
        tick: Some(tick.to_string()),
        price_rule: "fixed_tick".into(),
        settlement: "USDC".into(),
        market_id: Some(
            row["market_id"]
                .as_u64()
                .ok_or("missing Lighter market ID")?,
        ),
        filters: BTreeMap::from([
            (
                "LOT_SIZE".into(),
                serde_json::json!({"stepSize":step.to_string(),"minQty":positive("min_base_amount")?}),
            ),
            (
                "NOTIONAL".into(),
                serde_json::json!({"minNotional":positive("min_quote_amount")?,"maxNotional":positive("order_quote_limit")?}),
            ),
        ]),
    })
}
fn field(filter: &Value, key: &str) -> Result<Decimal> {
    let value = match filter.get(key) {
        None => Decimal::ZERO,
        Some(v) => d(v.as_str().ok_or("filter must be decimal string")?)?,
    };
    if value < Decimal::ZERO {
        return Err("negative instrument filter".into());
    }
    Ok(value)
}
pub fn combine(a: &Instrument, b: &Instrument) -> Result<Rules> {
    let mut steps = Vec::new();
    let mut minimum = Decimal::ZERO;
    let mut maximum: Option<Decimal> = None;
    let mut min_notional = Decimal::from(5);
    let mut max_notional: Option<Decimal> = None;
    for instrument in [a, b] {
        let lot = instrument
            .filters
            .get("LOT_SIZE")
            .ok_or("missing LOT_SIZE")?;
        if field(lot, "stepSize")? <= Decimal::ZERO {
            return Err("invalid LOT_SIZE step".into());
        }
        for name in ["LOT_SIZE", "MARKET_LOT_SIZE"] {
            if let Some(filter) = instrument.filters.get(name) {
                let step = field(filter, "stepSize")?;
                if step > Decimal::ZERO {
                    steps.push(step);
                }
                minimum = minimum.max(field(filter, "minQty")?);
                let max = field(filter, "maxQty")?;
                if max > Decimal::ZERO {
                    maximum = Some(maximum.map_or(max, |m| m.min(max)));
                }
            }
        }
        for name in ["MIN_NOTIONAL", "NOTIONAL"] {
            if let Some(filter) = instrument.filters.get(name) {
                min_notional = min_notional.max(field(
                    filter,
                    if filter.get("minNotional").is_some() {
                        "minNotional"
                    } else {
                        "notional"
                    },
                )?);
                let max = field(filter, "maxNotional")?;
                if max > Decimal::ZERO {
                    max_notional = Some(max_notional.map_or(max, |m| m.min(max)));
                }
            }
        }
    }
    let step = common_lot(&steps)
        .map_err(|e| e.to_string())?
        .ok_or("missing common lot")?;
    Ok(Rules {
        step: step.to_string(),
        minimum: minimum.max(step).to_string(),
        maximum: maximum.map(|x| x.to_string()),
        min_notional: min_notional.to_string(),
        max_notional: max_notional.map(|x| x.to_string()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn instrument(step: &str) -> Instrument {
        Instrument {
            symbol: "ETHUSDT".into(),
            tick: Some("0.01".into()),
            price_rule: "fixed_tick".into(),
            settlement: "USDT".into(),
            market_id: None,
            filters: BTreeMap::from([
                (
                    "LOT_SIZE".into(),
                    serde_json::json!({"stepSize":step,"minQty":"0.001","maxQty":"1000"}),
                ),
                (
                    "MARKET_LOT_SIZE".into(),
                    serde_json::json!({"stepSize":"0","minQty":"0","maxQty":"10"}),
                ),
                (
                    "NOTIONAL".into(),
                    serde_json::json!({"minNotional":"10","maxNotional":"5000"}),
                ),
            ]),
        }
    }
    #[test]
    fn combine_both_limit_and_market_filters() {
        let r = combine(&instrument("0.002"), &instrument("0.003")).unwrap();
        assert_eq!(d(&r.step).unwrap(), d("0.006").unwrap());
        assert_eq!(r.maximum.as_deref(), Some("10"));
        assert_eq!(r.min_notional, "10");
        assert_eq!(r.max_notional.as_deref(), Some("5000"));
    }
    #[test]
    fn missing_or_negative_rules_rejected() {
        let a = instrument("0.001");
        let mut b = a.clone();
        b.filters.remove("LOT_SIZE");
        assert!(combine(&a, &b).is_err());
        assert!(combine(&a, &instrument("-1")).is_err());
    }
    #[test]
    fn exact_symbol_and_base_required() {
        let info = serde_json::json!({"symbols":[{"symbol":"ETHUSDT","baseAsset":"1000ETH","quoteAsset":"USDT","status":"TRADING","isSpotTradingAllowed":true}]});
        assert!(parse(&info, "ETH", true).is_err());
    }
    #[test]
    fn hyperliquid_uses_dynamic_price_rule_and_common_lot() {
        let v = serde_json::json!({"collateralToken":0,"universe":[{"name":"ETH","szDecimals":4}]});
        let hl = hyperliquid(&v, "ETH").unwrap();
        assert_eq!(hl.symbol, "ETH");
        assert!(hl.tick.is_none());
        assert!(hl.price_rule.contains("Hyperliquid"));
        assert_eq!(combine(&instrument("0.001"), &hl).unwrap().step, "0.001");
        let mut bad = v.clone();
        bad["universe"][0]["isDelisted"] = serde_json::json!(true);
        assert!(hyperliquid(&bad, "ETH").is_err());
        bad = v;
        bad["collateralToken"] = serde_json::json!(1);
        assert!(hyperliquid(&bad, "ETH").is_err());
    }
    #[test]
    fn lighter_requires_active_exact_same_unit_perp_and_full_limits() {
        let row = serde_json::json!({"symbol":"ETH","market_type":"perp","status":"active","is_frozen":false,
            "market_config":{"force_reduce_only":false},"quote_asset_id":0,"multiplier":"1","market_id":0,
            "supported_size_decimals":4,"supported_price_decimals":2,"min_base_amount":"0.002",
            "min_quote_amount":"10","order_quote_limit":"100000"});
        let make = |r: Value| serde_json::json!({"order_book_details":[r]});
        let l = lighter(&make(row.clone()), "ETH").unwrap();
        assert_eq!(l.market_id, Some(0));
        assert_eq!(l.tick.as_deref(), Some("0.01"));
        let rules = combine(&instrument("0.001"), &l).unwrap();
        assert_eq!(rules.minimum, "0.002");
        assert_eq!(rules.max_notional.as_deref(), Some("5000"));
        for key in [
            "is_frozen",
            "multiplier",
            "min_base_amount",
            "market_id",
            "supported_size_decimals",
        ] {
            let mut bad = row.clone();
            bad[key] = Value::Null;
            assert!(lighter(&make(bad), "ETH").is_err(), "{key}");
        }
        assert!(lighter(&make(row.clone()), "BTC").is_err());
        assert!(lighter(
            &serde_json::json!({"order_book_details":[row.clone(),row]}),
            "ETH"
        )
        .is_err());
    }
}
