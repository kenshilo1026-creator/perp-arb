use crate::{Error, Result};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use zeroize::Zeroizing;

// Deliberately no Debug / Serialize: neither key, secret nor signed payload is logged.
pub struct Credentials {
    key: Zeroizing<String>,
    secret: Zeroizing<String>,
    account: String,
}
impl Credentials {
    pub fn new(key: &str, secret: &str) -> Result<Self> {
        if [key, secret].iter().any(|s| {
            !(16..=256).contains(&s.len()) || !s.bytes().all(|b| b.is_ascii_alphanumeric())
        }) {
            return Err(Error::Configuration);
        }
        Ok(Self {
            key: Zeroizing::new(key.into()),
            secret: Zeroizing::new(secret.into()),
            account: format!(
                "binance-key-{}",
                hex::encode(Sha256::digest(key.as_bytes()))
            ),
        })
    }
    pub fn from_env() -> Result<Self> {
        let prefix = if ["BINANCE_SPOT_API_KEY", "BINANCE_SPOT_API_SECRET"]
            .iter()
            .any(|k| std::env::var(k).is_ok_and(|v| !v.is_empty()))
        {
            "BINANCE_SPOT"
        } else {
            "BINANCE"
        };
        let key = Zeroizing::new(
            std::env::var(format!("{prefix}_API_KEY")).map_err(|_| Error::Configuration)?,
        );
        let secret = Zeroizing::new(
            std::env::var(format!("{prefix}_API_SECRET")).map_err(|_| Error::Configuration)?,
        );
        Self::new(&key, &secret)
    }
    pub fn account(&self) -> &str {
        &self.account
    }
    pub(crate) fn key(&self) -> &str {
        &self.key
    }
    pub(crate) fn sign(&self, payload: &str) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(self.secret.as_bytes())
            .expect("HMAC accepts any key length");
        mac.update(payload.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }
    pub(crate) fn signed(&self, params: &BTreeMap<String, String>) -> Result<String> {
        let payload = canonical(params)?;
        Ok(format!("{payload}&signature={}", self.sign(&payload)))
    }
}
/// Our wire contract only permits ASCII identifiers, integer/decimal values.
/// Reject anything needing URL escaping so signed bytes equal transmitted bytes.
pub(crate) fn canonical(params: &BTreeMap<String, String>) -> Result<String> {
    if params.iter().any(|(k, v)| {
        k.is_empty()
            || v.is_empty()
            || k == "signature"
            || !k.bytes().all(|c| c.is_ascii_alphanumeric())
            || !v
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"_-.".contains(&c))
    }) {
        return Err(Error::Configuration);
    }
    Ok(params
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&"))
}
