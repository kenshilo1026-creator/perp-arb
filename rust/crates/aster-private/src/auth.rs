//! Explicit credentials only. Never reads the environment or repository .env.
use crate::{Error, Result};
use k256::ecdsa::SigningKey;
use sha3::{Digest, Keccak256};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};
use zeroize::Zeroizing;

type Nonces = Mutex<HashMap<String, Arc<Mutex<u64>>>>;
static NONCES: OnceLock<Nonces> = OnceLock::new();

pub(crate) fn hash(bytes: impl AsRef<[u8]>) -> [u8; 32] {
    Keccak256::digest(bytes.as_ref()).into()
}

/// EIP-712 domain/message used by Aster v3; matches existing Python adapter.
pub fn signing_digest(encoded_params: &str) -> [u8; 32] {
    let mut domain = Vec::with_capacity(160);
    domain.extend(hash(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    ));
    domain.extend(hash(b"AsterSignTransaction"));
    domain.extend(hash(b"1"));
    let mut chain = [0u8; 32];
    chain[30..].copy_from_slice(&1666u16.to_be_bytes());
    domain.extend(chain);
    domain.extend([0u8; 32]);
    let mut message = Vec::with_capacity(64);
    message.extend(hash(b"Message(string msg)"));
    message.extend(hash(encoded_params.as_bytes()));
    let mut payload = Vec::with_capacity(66);
    payload.extend([0x19, 0x01]);
    payload.extend(hash(domain));
    payload.extend(hash(message));
    hash(payload)
}

/// Python urllib.parse.urlencode/quote_plus semantics, including UTF-8 symbols.
pub(crate) fn encode(pairs: &[(String, String)]) -> String {
    fn quote(text: &str) -> String {
        let mut result = String::new();
        for byte in text.bytes() {
            match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-' | b'.' | b'~' => {
                    result.push(byte as char)
                }
                b' ' => result.push('+'),
                _ => {
                    use std::fmt::Write;
                    let _ = write!(result, "%{byte:02X}");
                }
            }
        }
        result
    }
    pairs
        .iter()
        .map(|(k, v)| format!("{}={}", quote(k), quote(v)))
        .collect::<Vec<_>>()
        .join("&")
}

/// SigningKey zeroizes its secret on drop. No Debug implementation or public
/// arbitrary-request signer. Clone/share this object within one process.
pub struct Credentials {
    key: SigningKey,
    signer: String,
    user: String,
    nonce: Arc<Mutex<u64>>,
}
impl Credentials {
    pub fn new(private_key_hex: &str, user: &str) -> Result<Self> {
        if user.len() != 42
            || !user.starts_with("0x")
            || !user[2..].bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(Error::Credentials);
        }
        let key_hex = private_key_hex
            .strip_prefix("0x")
            .unwrap_or(private_key_hex);
        if key_hex.len() != 64 {
            return Err(Error::Credentials);
        }
        let raw = Zeroizing::new(hex::decode(key_hex).map_err(|_| Error::Credentials)?);
        let key = SigningKey::from_slice(&raw).map_err(|_| Error::Credentials)?;
        let pubkey = key.verifying_key().to_encoded_point(false);
        let signer = format!("0x{}", hex::encode(&hash(&pubkey.as_bytes()[1..])[12..]));
        let mut registry = NONCES
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .map_err(|_| Error::Credentials)?;
        if registry.len() >= 64 && !registry.contains_key(&signer) {
            return Err(Error::Configuration);
        }
        let nonce = registry
            .entry(signer.clone())
            .or_insert_with(|| Arc::new(Mutex::new(0)))
            .clone();
        Ok(Self {
            key,
            signer,
            user: user.to_lowercase(),
            nonce,
        })
    }
    pub fn user(&self) -> &str {
        &self.user
    }
    pub fn signer(&self) -> &str {
        &self.signer
    }

    pub(crate) fn next_nonce(&self, now_us: u64) -> Result<u64> {
        let mut previous = self.nonce.lock().map_err(|_| Error::Clock)?;
        let next = now_us.max(previous.checked_add(1).ok_or(Error::Clock)?);
        // A backward clock step must not create nonces outside Aster's window.
        if next.saturating_sub(now_us) > 50_000_000 {
            return Err(Error::Clock);
        }
        *previous = next;
        Ok(next)
    }
    pub(crate) fn sign(&self, params: Vec<(String, String)>) -> Result<String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::Clock)?
            .as_micros();
        let nonce = self.next_nonce(u64::try_from(now).map_err(|_| Error::Clock)?)?;
        self.sign_at(params, nonce)
    }
    pub(crate) fn sign_at(&self, mut params: Vec<(String, String)>, nonce: u64) -> Result<String> {
        params.extend([
            ("nonce".into(), nonce.to_string()),
            ("signer".into(), self.signer.clone()),
            ("user".into(), self.user.clone()),
        ]);
        let encoded = encode(&params);
        let (signature, recovery) = self
            .key
            .sign_prehash_recoverable(&signing_digest(&encoded))
            .map_err(|_| Error::Credentials)?;
        let mut bytes = signature.to_bytes().to_vec();
        bytes.push(recovery.to_byte() + 27);
        Ok(format!("{encoded}&signature=0x{}", hex::encode(bytes)))
    }
}
