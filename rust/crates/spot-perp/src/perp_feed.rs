//! Complete best-level quotes only. Never interpret incremental depth as a snapshot.
use crate::{
    d,
    market::Quote,
    metadata::Instrument,
    policy::{Book, Venue},
    Result,
};
use perp_arb_core::Decimal;
use serde_json::{json, Value};
use std::time::Instant;

pub fn subscriptions(venue: Venue, instrument: &Instrument) -> Result<Vec<Value>> {
    Ok(match venue {
        Venue::BinanceSpot | Venue::Aster => vec![],
        Venue::Hyperliquid => vec![
            json!({"method":"subscribe","subscription":{"type":"bbo","coin":instrument.symbol}}),
            json!({"method":"subscribe","subscription":{"type":"l2Book","coin":instrument.symbol,"fast":true}}),
        ],
        Venue::Lighter => vec![
            json!({"type":"subscribe","channel":format!("ticker/{}",instrument.market_id.ok_or("missing Lighter market ID")?)}),
        ],
    })
}

pub fn decode(v: &Value, venue: Venue, instrument: &Instrument) -> Result<Option<Quote>> {
    if v.get("error").is_some() || v["channel"] == "error" || v["type"] == "error" {
        return Err("public WS exchange error".into());
    }
    let (bid, ask, price_key, size_key, source_ms, version, snapshot) = match venue {
        Venue::Hyperliquid => {
            let channel = v["channel"].as_str().unwrap_or("");
            if !matches!(channel, "bbo" | "l2Book") {
                return Ok(None);
            }
            let data = &v["data"];
            if data["coin"] != instrument.symbol {
                return Err("wrong Hyperliquid coin".into());
            }
            let time = data["time"]
                .as_u64()
                .filter(|t| *t > 0)
                .ok_or("missing Hyperliquid source time")?;
            let (b, a) = if channel == "bbo" {
                (&data["bbo"][0], &data["bbo"][1])
            } else {
                (&data["levels"][0][0], &data["levels"][1][0])
            };
            (b, a, "px", "sz", time, time, channel == "l2Book")
        }
        Venue::Lighter => {
            if !matches!(
                v["type"].as_str(),
                Some("subscribed/ticker" | "update/ticker")
            ) {
                return Ok(None);
            }
            if v["channel"]
                != format!(
                    "ticker:{}",
                    instrument.market_id.ok_or("missing Lighter market ID")?
                )
                || v["ticker"]["s"] != instrument.symbol
            {
                return Err("wrong Lighter market/coin".into());
            }
            let time = v["timestamp"]
                .as_u64()
                .filter(|t| *t > 0)
                .ok_or("missing Lighter source time")?;
            // Timestamp identifies the complete BBO publication. last_updated_at is in
            // microseconds and can be older when the publisher repeats unchanged prices.
            let nonce = v["nonce"].as_u64().ok_or("missing Lighter quote nonce")?;
            (
                &v["ticker"]["b"],
                &v["ticker"]["a"],
                "price",
                "size",
                time,
                nonce,
                false,
            )
        }
        _ => return Err("wrong perp quote decoder".into()),
    };
    let field = |row: &Value, key: &str| -> Result<String> {
        let s = row[key].as_str().ok_or("missing decimal price/size")?;
        if d(s)? <= Decimal::ZERO {
            return Err("empty/nonpositive best level".into());
        }
        Ok(s.into())
    };
    let book = Book {
        bid: field(bid, price_key)?,
        ask: field(ask, price_key)?,
        bid_size: field(bid, size_key)?,
        ask_size: field(ask, size_key)?,
        age_ms: 0,
    };
    if d(&book.bid)? >= d(&book.ask)? {
        return Err("crossed perp best levels".into());
    }
    Ok(Some(Quote {
        book,
        version,
        received: Instant::now(),
        source_ms,
        snapshot,
        rest: false,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::Cache;
    fn instrument() -> Instrument {
        Instrument {
            symbol: "ETH".into(),
            tick: None,
            price_rule: "test".into(),
            settlement: "USDC".into(),
            market_id: Some(0),
            filters: Default::default(),
        }
    }
    fn hl(time: u64) -> Value {
        json!({"channel":"bbo","data":{"coin":"ETH","time":time,"bbo":[{"px":"100","sz":"2"},{"px":"101","sz":"3"}]}})
    }
    fn lighter(nonce: u64) -> Value {
        json!({"type":"update/ticker","channel":"ticker:0","nonce":nonce,"timestamp":1000,
            "ticker":{"s":"ETH","b":{"price":"100","size":"2"},"a":{"price":"101","size":"3"}}})
    }
    #[test]
    fn hyperliquid_bbo_and_depth_are_complete_quotes() {
        let i = instrument();
        let bbo = decode(&hl(1000), Venue::Hyperliquid, &i).unwrap().unwrap();
        assert_eq!(bbo.book.bid_size, "2");
        assert!(!bbo.snapshot);
        let full = json!({"channel":"l2Book","data":{"coin":"ETH","time":1000,"levels":[[{"px":"100.0","sz":"2.0"}],[{"px":"101","sz":"3"}]]}});
        let mut c = Cache::default();
        c.update(bbo).unwrap();
        assert!(c
            .update(decode(&full, Venue::Hyperliquid, &i).unwrap().unwrap())
            .unwrap());
        assert!(!c
            .update(decode(&hl(999), Venue::Hyperliquid, &i).unwrap().unwrap())
            .unwrap());
        let mut conflict = hl(1000);
        conflict["data"]["bbo"][0]["sz"] = json!("9");
        assert!(c
            .update(decode(&conflict, Venue::Hyperliquid, &i).unwrap().unwrap())
            .is_err());
        assert!(c.quote.is_none());
    }
    #[test]
    fn reject_missing_wrong_or_crossed_quotes() {
        let i = instrument();
        for mutation in 0..4 {
            let mut v = hl(1000);
            match mutation {
                0 => v["data"]["coin"] = json!("BTC"),
                1 => v["data"]["bbo"][0] = Value::Null,
                2 => v["data"]["bbo"][1]["px"] = json!("99"),
                _ => v["data"]["time"] = Value::Null,
            }
            assert!(decode(&v, Venue::Hyperliquid, &i).is_err());
        }
        for mutation in 0..5 {
            let mut v = lighter(2);
            match mutation {
                0 => v["channel"] = json!("ticker:1"),
                1 => v["ticker"]["s"] = json!("BTC"),
                2 => v["ticker"]["a"]["size"] = Value::Null,
                3 => v["ticker"]["b"]["size"] = json!("0"),
                _ => v["nonce"] = Value::Null,
            }
            assert!(decode(&v, Venue::Lighter, &i).is_err());
        }
    }
    #[test]
    fn lighter_full_ticker_does_not_need_contiguous_nonces_or_merge_depth() {
        let i = instrument();
        let mut c = Cache::default();
        for n in [2, 20] {
            assert!(c
                .update(decode(&lighter(n), Venue::Lighter, &i).unwrap().unwrap())
                .unwrap());
        }
        assert!(!c
            .update(decode(&lighter(2), Venue::Lighter, &i).unwrap().unwrap())
            .unwrap());
        assert!(!c
            .update(decode(&lighter(20), Venue::Lighter, &i).unwrap().unwrap())
            .unwrap());
        assert!(
            decode(&json!({"type":"update/order_book"}), Venue::Lighter, &i)
                .unwrap()
                .is_none()
        );
        assert_eq!(c.quote.unwrap().book.ask_size, "3");
    }
    #[test]
    fn subscriptions_never_fall_back_to_aster() {
        let i = instrument();
        let hl = subscriptions(Venue::Hyperliquid, &i).unwrap();
        assert_eq!(hl.len(), 2);
        assert_eq!(hl[0]["subscription"]["type"], "bbo");
        assert_eq!(hl[1]["subscription"]["fast"], true);
        assert_eq!(
            subscriptions(Venue::Lighter, &i).unwrap()[0]["channel"],
            "ticker/0"
        );
        let mut missing = i;
        missing.market_id = None;
        assert!(subscriptions(Venue::Lighter, &missing).is_err());
    }
}
