//! Explicit L1 order/cancel signer. No arbitrary actions, transfers, vaults or key logging.
use crate::{Error, Result};
use k256::ecdsa::SigningKey;
use perp_arb_private_events::orders::{Network, Order, TimeInForce};
use serde::Serialize;
use sha3::{Digest, Keccak256};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};
use zeroize::Zeroizing;
type Nonces = Mutex<HashMap<String, Arc<Mutex<u64>>>>;
static NONCES: OnceLock<Nonces> = OnceLock::new();
fn hash(v: impl AsRef<[u8]>) -> [u8; 32] {
    Keccak256::digest(v.as_ref()).into()
}
fn digest(connection: [u8; 32], network: Network) -> [u8; 32] {
    let mut domain = Vec::new();
    domain.extend(hash(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    ));
    domain.extend(hash(b"Exchange"));
    domain.extend(hash(b"1"));
    let mut chain = [0; 32];
    chain[30..].copy_from_slice(&1337u16.to_be_bytes());
    domain.extend(chain);
    domain.extend([0; 32]);
    let mut agent = Vec::new();
    agent.extend(hash(b"Agent(string source,bytes32 connectionId)"));
    agent.extend(hash(if network == Network::Mainnet {
        b"a"
    } else {
        b"b"
    }));
    agent.extend(connection);
    let mut payload = vec![0x19, 0x01];
    payload.extend(hash(domain));
    payload.extend(hash(agent));
    hash(payload)
}
#[derive(Serialize)]
struct Limit<'a> {
    tif: &'a str,
}
#[derive(Serialize)]
struct OrderType<'a> {
    limit: Limit<'a>,
}
#[derive(Serialize)]
struct Wire<'a> {
    a: u32,
    b: bool,
    p: String,
    s: String,
    r: bool,
    t: OrderType<'a>,
    c: &'a str,
}
#[derive(Serialize)]
struct OrderAction<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    orders: [Wire<'a>; 1],
    grouping: &'a str,
}
#[derive(Serialize)]
struct Cancel<'a> {
    asset: u32,
    cloid: &'a str,
}
#[derive(Serialize)]
struct CancelAction<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    cancels: [Cancel<'a>; 1],
}
#[derive(Serialize)]
struct Signature {
    r: String,
    s: String,
    v: u8,
}
#[derive(Serialize)]
struct Envelope<'a, T> {
    action: &'a T,
    nonce: u64,
    signature: Signature,
    #[serde(rename = "vaultAddress")]
    vault: Option<String>,
}
pub struct Credentials {
    key: SigningKey,
    address: String,
    nonce: Arc<Mutex<u64>>,
}
impl Credentials {
    pub(crate) fn isolated_leverage(
        &self,
        asset: u32,
        leverage: u32,
        network: Network,
    ) -> Result<String> {
        self.isolated_leverage_at(asset, leverage, network, self.nonce()?)
    }
    fn isolated_leverage_at(
        &self,
        asset: u32,
        leverage: u32,
        network: Network,
        nonce: u64,
    ) -> Result<String> {
        #[derive(Serialize)]
        struct Action {
            #[serde(rename = "type")]
            kind: &'static str,
            asset: u32,
            #[serde(rename = "isCross")]
            is_cross: bool,
            leverage: u32,
        }
        if asset >= 10000 || !(1..=125).contains(&leverage) {
            return Err(Error::Configuration);
        }
        self.encode(
            Action {
                kind: "updateLeverage",
                asset,
                is_cross: false,
                leverage,
            },
            nonce,
            network,
        )
    }
    pub fn new(secret: &str) -> Result<Self> {
        let s = secret.strip_prefix("0x").unwrap_or(secret);
        if s.len() != 64 {
            return Err(Error::Credentials);
        }
        let raw = Zeroizing::new(hex::decode(s).map_err(|_| Error::Credentials)?);
        let key = SigningKey::from_slice(&raw).map_err(|_| Error::Credentials)?;
        let address = format!(
            "0x{}",
            hex::encode(&hash(&key.verifying_key().to_encoded_point(false).as_bytes()[1..])[12..])
        );
        let mut registry = NONCES
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .map_err(|_| Error::Credentials)?;
        if registry.len() >= 64 && !registry.contains_key(&address) {
            return Err(Error::Capacity);
        }
        let nonce = registry.entry(address.clone()).or_default().clone();
        Ok(Self {
            key,
            address,
            nonce,
        })
    }
    pub fn address(&self) -> &str {
        &self.address
    }
    fn nonce(&self) -> Result<u64> {
        let now = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| Error::Clock)?
                .as_millis(),
        )
        .map_err(|_| Error::Clock)?;
        let mut last = self.nonce.lock().map_err(|_| Error::Clock)?;
        let next = now.max(last.checked_add(1).ok_or(Error::Clock)?);
        if next.saturating_sub(now) > 30_000 {
            return Err(Error::Clock);
        }
        *last = next;
        Ok(next)
    }
    fn encode<T: Serialize>(&self, action: T, nonce: u64, network: Network) -> Result<String> {
        let mut packed = rmp_serde::to_vec_named(&action).map_err(|_| Error::Protocol)?;
        packed.extend(nonce.to_be_bytes());
        packed.push(0); // No vault address.
        let (sig, recovery) = self
            .key
            .sign_prehash_recoverable(&digest(hash(packed), network))
            .map_err(|_| Error::Credentials)?;
        let bytes = sig.to_bytes();
        let signature = Signature {
            r: format!("0x{}", hex::encode(&bytes[..32])),
            s: format!("0x{}", hex::encode(&bytes[32..])),
            v: recovery.to_byte() + 27,
        };
        serde_json::to_string(&Envelope {
            action: &action,
            nonce,
            signature,
            vault: None,
        })
        .map_err(|_| Error::Protocol)
    }
    pub(crate) fn order(&self, o: &Order) -> Result<String> {
        self.order_at(o, self.nonce()?)
    }
    pub(crate) fn cancel(&self, o: &Order) -> Result<String> {
        self.cancel_at(o, self.nonce()?)
    }
    fn order_at(&self, o: &Order, nonce: u64) -> Result<String> {
        o.validate()?;
        if o.venue != "hyperliquid" {
            return Err(Error::Configuration);
        }
        self.encode(
            OrderAction {
                kind: "order",
                orders: [Wire {
                    a: o.asset.ok_or(Error::Configuration)?,
                    b: o.side == "BUY",
                    p: o.price()?.normalize().to_string(),
                    s: o.quantity()?.normalize().to_string(),
                    r: o.reduce_only,
                    t: OrderType {
                        limit: Limit {
                            tif: match o.tif {
                                TimeInForce::PostOnly => "Alo",
                                TimeInForce::Ioc => "Ioc",
                                TimeInForce::Market => return Err(Error::Configuration),
                            },
                        },
                    },
                    c: &o.client_id,
                }],
                grouping: "na",
            },
            nonce,
            o.network,
        )
    }
    fn cancel_at(&self, o: &Order, nonce: u64) -> Result<String> {
        o.validate()?;
        if o.venue != "hyperliquid" {
            return Err(Error::Configuration);
        }
        self.encode(
            CancelAction {
                kind: "cancelByCloid",
                cancels: [Cancel {
                    asset: o.asset.ok_or(Error::Configuration)?,
                    cloid: &o.client_id,
                }],
            },
            nonce,
            o.network,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn isolated_leverage_matches_python_adapter_signature() {
        let signer = Credentials::new(&format!("{:064x}", 1)).unwrap();
        let raw = signer
            .isolated_leverage_at(4, 3, Network::Mainnet, 1700000000000)
            .unwrap();
        let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(
            v["action"],
            serde_json::json!({"type":"updateLeverage","asset":4,"isCross":false,"leverage":3})
        );
        assert_eq!(
            v["signature"],
            serde_json::json!({"r":format!("0x{:0>64}","57b41ef2458f65bc54c8603ade31c3169efcaaa3bca7ddce8f02eed8ce3337c"),"s":format!("0x{:0>64}","7edf1756744d40a276f43eadb3308242219bd5aa5dfb17749a06e766f16b3335"),"v":28})
        );
        assert!(signer
            .isolated_leverage_at(4, 0, Network::Mainnet, 1)
            .is_err());
    }
    #[test]
    fn fixed_python_signing_vectors() {
        let rows: serde_json::Value =
            serde_json::from_str(include_str!("../fixtures/trade-signing.json")).unwrap();
        let signer = Credentials::new(&format!("{:064x}", 1)).unwrap();
        for row in rows.as_array().unwrap() {
            let o: Order = serde_json::from_value(row["order"].clone()).unwrap();
            let nonce = row["nonce"].as_u64().unwrap();
            let raw = if row["cancel"] == true {
                signer.cancel_at(&o, nonce)
            } else {
                signer.order_at(&o, nonce)
            }
            .unwrap();
            let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
            assert_eq!(v["signature"], row["signature"]);
            assert_eq!(v["action"], row["action"]);
        }
    }
}
