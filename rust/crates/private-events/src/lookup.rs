//! Typed read-only unknown-submission lookup. Absence never proves non-submission.
use crate::{aster, hyperliquid, Error, Result, Scope, Update};
use perp_arb_core::Decimal;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Selector {
    OrderId(String),
    ClientId(String),
}
#[derive(Debug, Clone)]
pub struct Request {
    pub scope: Scope,
    pub symbol: String,
    pub side: String,
    pub original: Decimal,
    pub selector: Selector,
}
#[derive(Debug, Clone)]
pub struct Found {
    pub scope: Scope,
    pub side: String,
    pub client_id: Option<String>,
    pub order: Update,
}
#[derive(Debug, Clone)]
pub enum Outcome {
    Found(Found),
    NotFound,
}
impl Request {
    pub fn validate(&self) -> Result<()> {
        if self.symbol.is_empty()
            || self.symbol.len() > 128
            || self.symbol.chars().any(char::is_control)
            || !matches!(self.side.as_str(), "BUY" | "SELL")
            || self.original <= Decimal::ZERO
        {
            return Err(Error::InvalidInput);
        }
        match (&*self.scope.venue, &self.selector) {
            ("aster" | "hyperliquid", Selector::OrderId(s)) if s.parse::<u64>().is_ok() => (),
            ("aster", Selector::ClientId(s))
                if !s.is_empty()
                    && s.len() <= 36
                    && s.bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-') => {}
            ("hyperliquid", Selector::ClientId(s))
                if s.len() == 34
                    && s.starts_with("0x")
                    && s[2..].bytes().all(|b| b.is_ascii_hexdigit()) => {}
            _ => return Err(Error::InvalidInput),
        }
        Ok(())
    }
    pub fn verify(&self, f: &Found) -> Result<()> {
        self.validate()?;
        f.order.validate()?;
        if f.scope != self.scope
            || f.side != self.side
            || f.order.key.symbol != self.symbol
            || f.order.original != self.original
        {
            return Err(Error::IdentityConflict);
        }
        match &self.selector {
            Selector::OrderId(id) if &f.order.key.order_id == id => Ok(()),
            Selector::ClientId(id) if f.client_id.as_ref() == Some(id) => Ok(()),
            _ => Err(Error::IdentityConflict),
        }
    }
}
pub fn aster_response(request: &Request, raw: &str) -> Result<Outcome> {
    if request.scope.venue != "aster" {
        return Err(Error::WrongSession);
    }
    if raw.len() > aster::MAX_FRAME_BYTES {
        return Err(Error::Capacity);
    }
    let v: Value = serde_json::from_str(raw).map_err(|_| Error::InvalidInput)?;
    let found = Found {
        scope: request.scope.clone(),
        side: v["side"].as_str().ok_or(Error::InvalidInput)?.into(),
        client_id: v["clientOrderId"].as_str().map(str::to_owned),
        order: aster::decode_rest(raw)?,
    };
    request.verify(&found)?;
    Ok(Outcome::Found(found))
}
pub fn hyperliquid_response(request: &Request, raw: &str) -> Result<Outcome> {
    request.validate()?;
    if request.scope.venue != "hyperliquid" {
        return Err(Error::WrongSession);
    }
    let v = hyperliquid::parse(raw)?;
    if v["status"] == "unknownOid" {
        return Ok(Outcome::NotFound);
    }
    let o = &v["order"]["order"];
    let side = match o["side"].as_str() {
        Some("B") => "BUY",
        Some("A") => "SELL",
        _ => return Err(Error::InvalidInput),
    };
    let found = Found {
        scope: request.scope.clone(),
        side: side.into(),
        client_id: o["cloid"].as_str().map(str::to_owned),
        order: hyperliquid::decode_rest(raw)?,
    };
    request.verify(&found)?;
    Ok(Outcome::Found(found))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn request(venue: &str) -> Request {
        Request {
            scope: Scope {
                venue: venue.into(),
                account: "fixture".into(),
            },
            symbol: if venue == "aster" { "ETHUSDT" } else { "ETH" }.into(),
            side: "BUY".into(),
            original: Decimal::TEN,
            selector: Selector::ClientId(
                if venue == "aster" {
                    "pa123"
                } else {
                    "0x00000000000000000000000000000001"
                }
                .into(),
            ),
        }
    }
    #[test]
    fn aster_binds_client_id_symbol_side_and_original_quantity() {
        let r = request("aster");
        let body = json!({"symbol":"ETHUSDT","orderId":2,"origQty":"10","executedQty":"2","status":"CANCELED","updateTime":100,"side":"BUY","clientOrderId":"pa123"});
        let Outcome::Found(f) = aster_response(&r, &body.to_string()).unwrap() else {
            panic!()
        };
        assert_eq!(f.order.cumulative, Decimal::from(2));
        for (field, value) in [
            ("symbol", json!("BTCUSDT")),
            ("side", json!("SELL")),
            ("clientOrderId", json!(null)),
            ("origQty", json!("11")),
        ] {
            let mut v = body.clone();
            v[field] = value;
            assert!(aster_response(&r, &v.to_string()).is_err());
        }
        assert!(aster_response(&r, r#"{"code":-2013}"#).is_err());
    }
    #[test]
    fn hyperliquid_cloid_and_oid_have_distinct_wire_identity() {
        let mut r = request("hyperliquid");
        let mut body = json!({"status":"order","order":{"order":{"coin":"ETH","oid":2,"origSz":"10","sz":"8","side":"B","cloid":"0x00000000000000000000000000000001"},"status":"canceled","statusTimestamp":100}});
        assert!(matches!(
            hyperliquid_response(&r, &body.to_string()),
            Ok(Outcome::Found(_))
        ));
        body["order"]["order"]["cloid"] = Value::Null;
        assert!(hyperliquid_response(&r, &body.to_string()).is_err());
        r.selector = Selector::OrderId("2".into());
        assert!(hyperliquid_response(&r, &body.to_string()).is_ok());
        body["order"]["order"]["side"] = json!("A");
        assert!(hyperliquid_response(&r, &body.to_string()).is_err());
        assert!(matches!(
            hyperliquid_response(&r, r#"{"status":"unknownOid"}"#),
            Ok(Outcome::NotFound)
        ));
    }
    #[test]
    fn invalid_selectors_are_rejected_before_transport() {
        for (venue, ids) in [
            ("aster", vec!["", "not safe", &"x".repeat(37)]),
            (
                "hyperliquid",
                vec!["123", "0x00", "0xgggggggggggggggggggggggggggggggg"],
            ),
        ] {
            for id in ids {
                let mut r = request(venue);
                r.selector = Selector::ClientId(id.into());
                assert!(r.validate().is_err());
            }
        }
        let mut r = request("aster");
        r.selector = Selector::OrderId("1 OR 2".into());
        assert!(r.validate().is_err());
    }
    #[test]
    fn verification_rejects_cross_account_and_malformed_fill() {
        let r = request("aster");
        let mut f = Found {
            scope: r.scope.clone(),
            side: r.side.clone(),
            client_id: Some("pa123".into()),
            order: Update {
                key: crate::OrderKey {
                    symbol: r.symbol.clone(),
                    order_id: "2".into(),
                },
                original: Decimal::TEN,
                cumulative: Decimal::ONE,
                status: crate::Status::PartiallyFilled,
                event_ms: Some(100),
            },
        };
        assert!(r.verify(&f).is_ok());
        f.scope.account = "other".into();
        assert!(r.verify(&f).is_err());
        f.scope = r.scope.clone();
        f.order.cumulative = Decimal::from(11);
        assert!(r.verify(&f).is_err());
    }
}
