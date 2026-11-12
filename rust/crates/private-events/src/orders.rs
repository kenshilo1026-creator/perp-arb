//! Explicit single-order wire contract. No market/trigger/transfer actions.
use crate::{
    lookup::{Request, Selector},
    Error, Result, Scope,
};
use perp_arb_core::{decimal, Decimal};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Network {
    Mainnet,
    Testnet,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TimeInForce {
    PostOnly,
    Ioc,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Order {
    pub venue: String,
    pub network: Network,
    pub account: String,
    pub symbol: String,
    pub side: String,
    pub quantity: String,
    pub limit_price: String,
    pub tif: TimeInForce,
    pub reduce_only: bool,
    pub client_id: String,
    pub asset: Option<u32>,
}
impl Order {
    pub fn quantity(&self) -> Result<Decimal> {
        decimal(&self.quantity).map_err(|_| Error::InvalidQuantity)
    }
    pub fn price(&self) -> Result<Decimal> {
        decimal(&self.limit_price).map_err(|_| Error::InvalidQuantity)
    }
    pub fn request(&self) -> Result<Request> {
        let r = Request {
            scope: Scope {
                venue: self.venue.clone(),
                account: self.account.clone(),
            },
            symbol: self.symbol.clone(),
            side: self.side.clone(),
            original: self.quantity()?,
            selector: Selector::ClientId(self.client_id.clone()),
        };
        r.validate()?;
        Ok(r)
    }
    pub fn validate(&self) -> Result<()> {
        self.request()?;
        if self.price()? <= Decimal::ZERO
            || self.account.len() != 42
            || !self.account.starts_with("0x")
            || !self.account[2..].bytes().all(|b| b.is_ascii_hexdigit())
            || self.symbol.contains([':', '/', '@'])
        {
            return Err(Error::InvalidInput);
        }
        match self.venue.as_str() {
            "aster" if self.network == Network::Mainnet && self.asset.is_none() => (),
            "hyperliquid" if self.asset.is_some_and(|a| a < 10000) => (),
            _ => return Err(Error::InvalidInput),
        }
        Ok(())
    }
}
/// ACK evidence is provisional: preserve explicit fills, but reconcile terminal status separately.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Accepted {
    pub order_id: String,
    pub cumulative: Option<String>,
}
