use crate::{number, Error, Result};
use perp_arb_core::Decimal;
use perp_arb_private_events::{
    lookup::{Request, Selector},
    Scope,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Kind {
    Maker { price: String },
    Market,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Intent {
    pub symbol: String,
    pub side: String,
    pub quantity: String,
    pub client_id: String,
    pub kind: Kind,
}
pub(crate) fn symbol(s: &str) -> bool {
    s.len() > 4
        && s.len() <= 28
        && s.ends_with("USDT")
        && s.bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
}
pub(crate) fn client_id(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 36
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
}
impl Intent {
    pub fn validate(&self) -> Result<()> {
        if !symbol(&self.symbol)
            || !matches!(self.side.as_str(), "BUY" | "SELL")
            || !client_id(&self.client_id)
            || number(&self.quantity)? <= Decimal::ZERO
        {
            return Err(Error::Configuration);
        }
        if let Kind::Maker { price } = &self.kind {
            if number(price)? <= Decimal::ZERO {
                return Err(Error::Configuration);
            }
        }
        Ok(())
    }
    pub fn request(&self, scope: Scope) -> Result<Request> {
        self.validate()?;
        if scope.venue != "binance_spot" {
            return Err(Error::Identity);
        }
        Ok(Request {
            scope,
            symbol: self.symbol.clone(),
            side: self.side.clone(),
            original: number(&self.quantity)?,
            selector: Selector::ClientId(self.client_id.clone()),
        })
    }
    pub(crate) fn params(&self) -> Result<BTreeMap<String, String>> {
        self.validate()?;
        let mut p = BTreeMap::from([
            ("symbol".into(), self.symbol.clone()),
            ("side".into(), self.side.clone()),
            (
                "quantity".into(),
                number(&self.quantity)?.normalize().to_string(),
            ),
            ("newClientOrderId".into(), self.client_id.clone()),
            ("newOrderRespType".into(), "RESULT".into()),
        ]);
        match &self.kind {
            Kind::Maker { price } => {
                p.insert("type".into(), "LIMIT_MAKER".into());
                p.insert("price".into(), number(price)?.normalize().to_string());
            }
            Kind::Market => {
                p.insert("type".into(), "MARKET".into());
            }
        }
        Ok(p)
    }
}
