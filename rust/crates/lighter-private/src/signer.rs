//! Minimal ABI for the pinned official Lighter signer, never a Python subprocess.
//! The SHA-256 check is mandatory: old DLLs export these names with incompatible ABIs.
use crate::Result;
use libloading::Library;
use sha2::{Digest, Sha256};
use std::{
    ffi::{c_char, c_void, CStr, CString},
    path::Path,
    sync::{Mutex, OnceLock},
};
#[repr(C)]
struct Signed {
    kind: u8,
    info: *mut c_void,
    hash: *mut c_void,
    message: *mut c_void,
    error: *mut c_void,
}
#[repr(C)]
struct Token {
    text: *mut c_void,
    error: *mut c_void,
}
type Create = unsafe extern "C" fn(*const c_char, *const c_char, i32, i32, i64) -> *mut c_void;
type Free = unsafe extern "C" fn(*mut c_void);
type SignOrder = unsafe extern "C" fn(
    i32,
    i64,
    i64,
    i32,
    i32,
    i32,
    i32,
    i32,
    i32,
    i64,
    i64,
    i32,
    i32,
    u8,
    u8,
    u8,
    i64,
    i32,
    i64,
) -> Signed;
type Cancel = unsafe extern "C" fn(i32, i64, u8, i64, i32, i64) -> Signed;
type Leverage = unsafe extern "C" fn(i32, i32, i32, u8, i64, i32, i64) -> Signed;
type Auth = unsafe extern "C" fn(i64, i32, i64) -> Token;
// Go's runtime owns background threads: deliberately keep this library loaded for process lifetime.
static LIBRARY: OnceLock<Library> = OnceLock::new();
static LOAD: Mutex<()> = Mutex::new(());
pub struct Signer {
    lib: &'static Library,
    account: i64,
    key: i32,
}
pub struct Wire {
    pub kind: u8,
    pub info: String,
}
impl Signer {
    pub fn load(path: &Path, secret: &str, account: i64, key: i32) -> Result<Self> {
        if !cfg!(all(target_os = "windows", target_arch = "x86_64")) {
            return Err("Lighter signer currently supports Windows x64 only".into());
        }
        let bytes = std::fs::read(path)
            .map_err(|_| "missing Lighter signer; run scripts/setup_lighter_signer.py")?;
        let hash = format!("{:x}", Sha256::digest(&bytes));
        if hash != "6e5ede71ecc3f74002752421f823d9493459ade7f6e01b37539b5be4c4299b66" {
            return Err("Lighter signer checksum/ABI mismatch".into());
        }
        let url = CString::new(perp_arb_market_data::lighter::BASE).unwrap();
        if secret.as_bytes().contains(&0) {
            return Err("invalid Lighter key".into());
        }
        let mut private = zeroize::Zeroizing::new(secret.as_bytes().to_vec());
        private.push(0);
        // SAFETY: fixed architecture and hash identify the documented C ABI. Library stays
        // owned until every call completes. Each result allocation is freed by its own CRT.
        unsafe {
            let _load = LOAD.lock().map_err(|_| "signer loader poisoned")?;
            if LIBRARY.get().is_none() {
                let lib = Library::new(path.canonicalize().map_err(|_| "signer path")?)
                    .map_err(|_| "Lighter signer load failed")?;
                let _ = LIBRARY.set(lib);
            }
            let lib = LIBRARY.get().ok_or("signer library unavailable")?;
            let create = lib
                .get::<Create>(b"CreateClient\0")
                .map_err(|_| "signer ABI")?;
            let error = create(url.as_ptr(), private.as_ptr().cast(), 304, key, account);
            let signer = Self { lib, account, key };
            if signer.take(error)?.is_some() {
                return Err("Lighter signer initialization failed".into());
            }
            Ok(signer)
        }
    }
    unsafe fn take(&self, p: *mut c_void) -> Result<Option<String>> {
        if p.is_null() {
            return Ok(None);
        }
        let value = CStr::from_ptr(p.cast())
            .to_str()
            .map(str::to_owned)
            .map_err(|_| "signer returned invalid UTF-8".to_string());
        self.lib.get::<Free>(b"Free\0").map_err(|_| "signer ABI")?(p);
        value.map(Some)
    }
    unsafe fn wire(&self, s: Signed) -> Result<Wire> {
        // Read/free ALL fields before propagating any error, including unused tx hash.
        let info = self.take(s.info);
        let hash = self.take(s.hash);
        let message = self.take(s.message);
        let error = self.take(s.error);
        let _ = hash?;
        let _ = message?;
        if error?.is_some() {
            return Err("Lighter transaction signing rejected".into());
        }
        Ok(Wire {
            kind: s.kind,
            info: info?.ok_or("missing signed transaction")?,
        })
    }
    pub fn auth(&self, deadline: i64) -> Result<String> {
        unsafe {
            let s = self
                .lib
                .get::<Auth>(b"CreateAuthToken\0")
                .map_err(|_| "signer ABI")?(deadline, self.key, self.account);
            let text = self.take(s.text);
            let error = self.take(s.error);
            if error?.is_some() {
                return Err("Lighter auth signing failed".into());
            }
            text?.ok_or("missing auth token".into())
        }
    }
    pub fn order(&self, order: &crate::OrderWire, nonce: i64) -> Result<Wire> {
        unsafe {
            let s = self
                .lib
                .get::<SignOrder>(b"SignCreateOrder\0")
                .map_err(|_| "signer ABI")?(
                order.market as i32,
                order.client,
                order.size,
                order.price as i32,
                order.ask,
                0,
                order.tif,
                order.reduce,
                0,
                order.expiry,
                0,
                0,
                0,
                0,
                0,
                0,
                nonce,
                self.key,
                self.account,
            );
            self.wire(s)
        }
    }
    pub fn cancel(&self, market: i64, id: i64, nonce: i64) -> Result<Wire> {
        unsafe {
            let s = self
                .lib
                .get::<Cancel>(b"SignCancelOrder\0")
                .map_err(|_| "signer ABI")?(
                market as i32, id, 0, nonce, self.key, self.account
            );
            self.wire(s)
        }
    }
    pub fn leverage(&self, market: i64, fraction: i32, nonce: i64) -> Result<Wire> {
        unsafe {
            let s = self
                .lib
                .get::<Leverage>(b"SignUpdateLeverage\0")
                .map_err(|_| "signer ABI")?(
                market as i32,
                fraction,
                1,
                0,
                nonce,
                self.key,
                self.account,
            );
            self.wire(s)
        }
    }
}
