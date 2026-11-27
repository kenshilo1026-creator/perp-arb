//! Native Lighter execution. Unknown sends are reconciled by client index, never retried.
#![deny(unsafe_code)]
pub mod monitor;
#[allow(unsafe_code)]
mod signer;
#[cfg(test)]
mod tests;
use perp_arb_core::Decimal as D;
use perp_arb_market_data::{
    catalog::ResolvedInstrument,
    lighter::{dec, BASE},
};
use perp_arb_private_events::{
    account,
    actor::Coordinator,
    lookup::{Found, Outcome, Request, Selector},
    orders::{Accepted, Order, TimeInForce},
    OrderKey, Scope, Status, Update,
};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::{watch, Mutex as AsyncMutex};
use zeroize::Zeroizing;
pub type Result<T> = std::result::Result<T, String>;
pub fn now() -> i64 {
    perp_arb_market_data::hub::wall_ms()
}
#[derive(Clone)]
pub struct Client(Arc<Inner>);
struct Inner {
    signer: Mutex<signer::Signer>,
    http: reqwest::Client,
    account: String,
    key: i32,
    spec: ResolvedInstrument,
    nonce: AsyncMutex<Option<i64>>,
    read_gate: AsyncMutex<Instant>,
    trade_gate: AsyncMutex<Instant>,
    tracked: Mutex<BTreeMap<String, Request>>,
    found: Mutex<BTreeMap<String, Found>>,
    revision: watch::Sender<u64>,
}
pub struct OrderWire {
    pub market: i64,
    pub client: i64,
    pub size: i64,
    pub price: u32,
    pub ask: i32,
    pub tif: i32,
    pub reduce: i32,
    pub expiry: i64,
}
pub fn order_wire(o: &Order, spec: &ResolvedInstrument) -> Result<OrderWire> {
    o.validate().map_err(|e| e.to_string())?;
    if o.venue != "lighter"
        || spec.key.symbol() != o.symbol
        || spec.asset_id.map(|v| v as u32) != o.asset
    {
        return Err("Lighter instrument identity mismatch".into());
    }
    let q = o.quantity().map_err(|e| e.to_string())?;
    let p = o.price().map_err(|e| e.to_string())?;
    if spec
        .limit
        .size_error(q, Some(p))
        .map_err(|e| e.to_string())?
        .is_some()
        || spec
            .limit
            .price_error(p)
            .map_err(|e| e.to_string())?
            .is_some()
    {
        return Err("Lighter size/price constraints".into());
    }
    let perp_arb_core::instruments::PriceRule::Tick(tick) = spec.limit.price_rule() else {
        return Err("Lighter tick missing".into());
    };
    let scaled = |v: D, step: D| -> Result<String> {
        let n = v.checked_div(step).ok_or("scaled amount overflow")?;
        if !n.fract().is_zero() {
            return Err("fractional wire amount".into());
        }
        Ok(n.normalize().to_string())
    };
    Ok(OrderWire {
        market: i64::from(o.asset.unwrap()),
        client: o.client_id.parse().map_err(|_| "client index")?,
        size: scaled(q, spec.limit.lot())?
            .parse()
            .map_err(|_| "Lighter size overflow")?,
        price: scaled(p, tick)?
            .parse()
            .map_err(|_| "Lighter price overflow")?,
        ask: i32::from(o.side == "SELL"),
        tif: if o.tif == TimeInForce::PostOnly { 2 } else { 0 },
        reduce: i32::from(o.reduce_only),
        expiry: if o.tif == TimeInForce::PostOnly {
            now() + 120_000
        } else {
            0
        },
    })
}
pub fn decode_order(row: &Value, scope: &Scope, symbol: &str, market: u64) -> Result<Found> {
    if row["owner_account_index"]
        .as_u64()
        .map(|n| n.to_string())
        .as_ref()
        != Some(&scope.account)
        || row["market_index"].as_u64() != Some(market)
    {
        return Err("Lighter order account/market mismatch".into());
    }
    let id = row["order_index"]
        .as_u64()
        .filter(|n| *n > 0)
        .ok_or("missing exchange order index")?
        .to_string();
    let cid = row["client_order_index"]
        .as_u64()
        .ok_or("missing client order index")?
        .to_string();
    if row.get("order_id").is_some_and(|v| v.as_str() != Some(&id))
        || row
            .get("client_order_id")
            .is_some_and(|v| v.as_str() != Some(&cid))
    {
        return Err("conflicting Lighter order identity".into());
    }
    let original = dec(&row["initial_base_amount"])?;
    let cumulative = dec(&row["filled_base_amount"])?;
    let remaining = dec(&row["remaining_base_amount"])?;
    if original - cumulative != remaining {
        return Err("inconsistent Lighter filled/remaining amount".into());
    }
    let status = match row["status"].as_str() {
        Some("filled") => Status::Filled,
        Some(s) if s == "canceled" || s.starts_with("canceled-") => Status::Canceled,
        Some("open") if cumulative > D::ZERO => Status::PartiallyFilled,
        Some("open") => Status::New,
        Some("pending" | "in-progress") if cumulative == D::ZERO => Status::Pending,
        _ => return Err("unsupported Lighter order status".into()),
    };
    let event_ms = row["updated_at"]
        .as_u64()
        .filter(|n| *n > 0)
        .or_else(|| row["timestamp"].as_u64().filter(|n| *n > 0));
    let found = Found {
        scope: scope.clone(),
        side: if row["is_ask"].as_bool().ok_or("missing order side")? {
            "SELL"
        } else {
            "BUY"
        }
        .into(),
        client_id: Some(cid),
        order: Update {
            key: OrderKey {
                symbol: symbol.into(),
                order_id: id,
            },
            original,
            cumulative,
            status,
            event_ms,
        },
    };
    found.order.validate().map_err(|e| e.to_string())?;
    Ok(found)
}
impl Client {
    pub async fn connect(o: &Order) -> Result<Self> {
        o.validate().map_err(|e| e.to_string())?;
        let account: i64 = std::env::var("LIGHTER_ACCOUNT_INDEX")
            .map_err(|_| "missing LIGHTER_ACCOUNT_INDEX")?
            .parse()
            .map_err(|_| "invalid account index")?;
        if account.to_string() != o.account {
            return Err("Lighter configured account does not match plan".into());
        }
        let key: i32 = std::env::var("LIGHTER_API_KEY_INDEX")
            .map_err(|_| "missing LIGHTER_API_KEY_INDEX")?
            .parse()
            .map_err(|_| "invalid API key index")?;
        if !(3..=254).contains(&key) {
            return Err("Lighter API key index must be 3..254".into());
        }
        let secret = Zeroizing::new(
            std::env::var("API_KEY_PRIVATE_KEY")
                .or_else(|_| std::env::var("LIGHTER_PRIVATE_KEY"))
                .map_err(|_| "missing Lighter private key")?,
        );
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../vendor/lighter/lighter-signer-windows-amd64.dll");
        let signer = signer::Signer::load(&path, &secret, account, key)?;
        let spec = perp_arb_market_data::lighter::resolve(&o.symbol).await?;
        order_wire(o, &spec)?;
        let (revision, _) = watch::channel(0);
        let c = Self(Arc::new(Inner {
            signer: Mutex::new(signer),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(8))
                .pool_idle_timeout(Duration::from_secs(90))
                .build()
                .map_err(|_| "Lighter HTTP client")?,
            account: o.account.clone(),
            key,
            spec,
            nonce: AsyncMutex::new(None),
            read_gate: AsyncMutex::new(Instant::now()),
            trade_gate: AsyncMutex::new(Instant::now()),
            tracked: Mutex::new(BTreeMap::new()),
            found: Mutex::new(BTreeMap::new()),
            revision,
        }));
        // Authenticated read verifies key/account permissions before any mutation.
        c.active_orders().await?;
        let next = c
            .get(
                "nextNonce",
                vec![
                    ("account_index", c.0.account.clone()),
                    ("api_key_index", key.to_string()),
                ],
                false,
            )
            .await?["nonce"]
            .as_i64()
            .filter(|n| *n >= 0)
            .ok_or("missing nonce")?;
        *c.0.nonce.lock().await = Some(next);
        Ok(c)
    }
    pub fn scope(&self) -> Scope {
        Scope {
            venue: "lighter".into(),
            account: self.0.account.clone(),
        }
    }
    fn auth(&self) -> Result<String> {
        self.0
            .signer
            .lock()
            .map_err(|_| "signer poisoned")?
            .auth(now() / 1000 + 600)
    }
    async fn reserve(gate: &AsyncMutex<Instant>, spacing: Duration) {
        let mut next = gate.lock().await;
        tokio::time::sleep_until((*next).into()).await;
        *next = Instant::now() + spacing;
    }
    async fn response(response: reqwest::Response) -> Result<Value> {
        let status = response.status();
        if !status.is_success() {
            return Err(format!(
                "Lighter HTTP {}; request outcome may be unknown",
                status.as_u16()
            ));
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|_| "Lighter response transport")?;
        if bytes.len() > 4_000_000 {
            return Err("Lighter response capacity".into());
        }
        let v: Value = serde_json::from_slice(&bytes).map_err(|_| "Lighter invalid JSON")?;
        if v["code"] != 200 {
            return Err(format!(
                "Lighter rejected request code={}",
                v["code"].as_i64().unwrap_or(-1)
            ));
        }
        Ok(v)
    }
    async fn get(&self, path: &str, params: Vec<(&str, String)>, auth: bool) -> Result<Value> {
        Self::reserve(&self.0.read_gate, Duration::from_millis(1050)).await;
        let mut request = self
            .0
            .http
            .get(format!("{BASE}/api/v1/{path}"))
            .query(&params);
        if auth {
            request = request.header("Authorization", self.auth()?);
        }
        let response = request.send().await.map_err(|_| "Lighter read transport")?;
        if response.status().as_u16() == 429 {
            *self.0.read_gate.lock().await = Instant::now() + Duration::from_secs(60);
        }
        Self::response(response).await
    }
    async fn transact(
        &self,
        sign: impl FnOnce(&signer::Signer, i64) -> Result<signer::Wire>,
    ) -> Result<()> {
        let mut nonce = self.0.nonce.lock().await;
        Self::reserve(&self.0.trade_gate, Duration::from_millis(1600)).await;
        let n = match *nonce {
            Some(n) => n,
            None => self
                .get(
                    "nextNonce",
                    vec![
                        ("account_index", self.0.account.clone()),
                        ("api_key_index", self.0.key.to_string()),
                    ],
                    false,
                )
                .await?["nonce"]
                .as_i64()
                .filter(|n| *n >= 0)
                .ok_or("missing nonce")?,
        };
        let wire = {
            let signer = self.0.signer.lock().map_err(|_| "signer poisoned")?;
            sign(&signer, n)?
        };
        // Reserve BEFORE transport. On timeout the nonce is not reused; lookup owns recovery.
        *nonce = Some(n.checked_add(1).ok_or("nonce overflow")?);
        let form = reqwest::multipart::Form::new()
            .text("tx_type", wire.kind.to_string())
            .text("tx_info", wire.info);
        let result = match self
            .0
            .http
            .post(format!("{BASE}/api/v1/sendTx"))
            .multipart(form)
            .send()
            .await
        {
            Ok(r) => {
                if r.status().as_u16() == 429 {
                    *self.0.trade_gate.lock().await = Instant::now() + Duration::from_secs(60);
                }
                Self::response(r).await.map(|_| ())
            }
            Err(_) => Err("Lighter send outcome unknown; reconcile client index".into()),
        };
        if result.is_err() {
            *nonce = None;
        }
        result
    }
    fn remember(&self, f: Found) -> Result<()> {
        let id = f.client_id.clone().ok_or("client ID missing")?;
        let mut map = self.0.found.lock().map_err(|_| "lookup cache poisoned")?;
        if let Some(old) = map.get(&id) {
            if old.order.key != f.order.key
                || old.order.original != f.order.original
                || old.side != f.side
            {
                return Err("Lighter cached identity conflict".into());
            }
            if f.order.cumulative < old.order.cumulative {
                return Ok(());
            }
        }
        if map.len() >= 8192 && !map.contains_key(&id) {
            return Err("Lighter order cache capacity".into());
        }
        map.insert(id, f);
        self.0.revision.send_modify(|n| *n = n.wrapping_add(1));
        Ok(())
    }
    pub async fn lookup(&self, r: &Request) -> Result<Outcome> {
        r.validate().map_err(|e| e.to_string())?;
        if r.scope != self.scope() || r.symbol != self.0.spec.key.symbol() {
            return Err("Lighter lookup scope mismatch".into());
        }
        let Selector::ClientId(id) = &r.selector else {
            return Err("Lighter exact lookup requires client index".into());
        };
        {
            let mut tracked = self.0.tracked.lock().map_err(|_| "tracked poisoned")?;
            if tracked.len() >= 8192 && !tracked.contains_key(id) {
                return Err("Lighter tracked capacity".into());
            }
            tracked.insert(id.clone(), r.clone());
        }
        let v = self
            .get(
                "accountOrders",
                vec![
                    ("account_index", self.0.account.clone()),
                    ("client_order_indexes", id.clone()),
                ],
                true,
            )
            .await?;
        let rows = v["orders"].as_array().ok_or("missing Lighter orders")?;
        if rows.is_empty() {
            return self.lookup_archive(r, id).await;
        }
        if rows.len() != 1 {
            return Err("ambiguous Lighter lookup".into());
        }
        let f = decode_order(
            &rows[0],
            &r.scope,
            &r.symbol,
            self.0.spec.asset_id.unwrap() as u64,
        )?;
        r.verify(&f).map_err(|e| e.to_string())?;
        self.remember(f.clone())?;
        Ok(Outcome::Found(f))
    }
    async fn lookup_archive(&self, r: &Request, id: &str) -> Result<Outcome> {
        // Older terminal orders may be absent from accountOrders; missing never means rejected.
        let market = self.0.spec.asset_id.unwrap().to_string();
        let active = self
            .get(
                "accountActiveOrders",
                vec![
                    ("account_index", self.0.account.clone()),
                    ("market_id", market.clone()),
                ],
                true,
            )
            .await?;
        if let Some(f) = find_exact(&active, r, self.0.spec.asset_id.unwrap() as u64, id)? {
            self.remember(f.clone())?;
            return Ok(Outcome::Found(f));
        }
        let mut cursor = String::new();
        for _ in 0..5 {
            let mut params = vec![
                ("account_index", self.0.account.clone()),
                ("market_id", market.clone()),
                ("limit", "100".into()),
            ];
            if !cursor.is_empty() {
                params.push(("cursor", cursor.clone()));
            }
            let page = self.get("accountInactiveOrders", params, true).await?;
            if let Some(f) = find_exact(&page, r, self.0.spec.asset_id.unwrap() as u64, id)? {
                self.remember(f.clone())?;
                return Ok(Outcome::Found(f));
            }
            let Some(next) = page["next_cursor"].as_str().filter(|s| !s.is_empty()) else {
                break;
            };
            if next == cursor || next.len() > 2048 {
                return Err("Lighter history cursor invalid".into());
            }
            cursor = next.into();
        }
        Ok(Outcome::NotFound)
    }
    pub async fn submit(&self, o: &Order) -> Result<Accepted> {
        if o.account != self.0.account {
            return Err("Lighter submit account mismatch".into());
        }
        let wire = order_wire(o, &self.0.spec)?;
        let request = o.request().map_err(|e| e.to_string())?;
        {
            let mut tracked = self
                .0
                .tracked
                .lock()
                .map_err(|_| "tracked orders poisoned")?;
            if tracked.len() >= 8192 || tracked.contains_key(&o.client_id) {
                return Err("duplicate Lighter submit intent".into());
            }
            tracked.insert(o.client_id.clone(), request.clone());
        }
        let mut revision = self.0.revision.subscribe();
        self.transact(|s, n| s.order(&wire, n)).await?;
        let ack = tokio::time::timeout(Duration::from_millis(800), async {
            loop {
                if let Some(f) = self
                    .0
                    .found
                    .lock()
                    .map_err(|_| "cache poisoned")?
                    .get(&o.client_id)
                    .cloned()
                {
                    request.verify(&f).map_err(|e| e.to_string())?;
                    return Ok::<_, String>(f);
                }
                revision
                    .changed()
                    .await
                    .map_err(|_| "order stream closed")?;
            }
        })
        .await;
        let f=match ack{Ok(r)=>r?,Err(_)=>match self.lookup(&request).await?{Outcome::Found(f)=>f,Outcome::NotFound=>return Err("Lighter transaction accepted; real order identity pending; lookup without resubmit".into())}};
        Ok(Accepted {
            order_id: f.order.key.order_id,
            cumulative: Some(f.order.cumulative.to_string()),
        })
    }
    pub async fn cancel(&self, o: &Order) -> Result<()> {
        if o.account != self.0.account {
            return Err("Lighter cancel scope mismatch".into());
        }
        order_wire(o, &self.0.spec)?;
        // Resolve real order index first, never confuse a tx hash with an order ID.
        let Outcome::Found(f) = self
            .lookup(&o.request().map_err(|e| e.to_string())?)
            .await?
        else {
            return Err("unknown Lighter order; cannot prove cancel".into());
        };
        if f.order.status.terminal() {
            return Ok(());
        }
        let id = f
            .order
            .key
            .order_id
            .parse()
            .map_err(|_| "order index overflow")?;
        self.transact(|s, n| s.cancel(o.asset.unwrap().into(), id, n))
            .await
    }
    pub async fn configure_isolated(&self, o: &Order, leverage: u32) -> Result<()> {
        order_wire(o, &self.0.spec)?;
        if o.account != self.0.account || leverage == 0 || leverage > 125 {
            return Err("invalid leverage/account".into());
        }
        // Round margin fraction UP so effective leverage never exceeds the request.
        let fraction = 10000u32.div_ceil(leverage) as i32;
        self.transact(|s, n| s.leverage(o.asset.unwrap().into(), fraction, n))
            .await?;
        for _ in 0..5 {
            let a = self.account().await?;
            let positions = a["positions"].as_array().ok_or("positions missing")?;
            if positions.iter().any(|p| {
                p["market_id"].as_u64() == o.asset.map(u64::from)
                    && p["margin_mode"] == 1
                    && dec(&p["initial_margin_fraction"])
                        .is_ok_and(|n| n == D::new(i64::from(fraction), 2))
            }) {
                return Ok(());
            }
        }
        Err("Lighter margin/leverage not confirmed; no maker dispatched".into())
    }
    async fn account(&self) -> Result<Value> {
        let v = self
            .get(
                "account",
                vec![("by", "index".into()), ("value", self.0.account.clone())],
                false,
            )
            .await?;
        let a = v["accounts"]
            .as_array()
            .filter(|a| a.len() == 1)
            .ok_or("Lighter account missing/ambiguous")?[0]
            .clone();
        if a["account_index"].as_u64().map(|n| n.to_string()) != Some(self.0.account.clone()) {
            return Err("Lighter account mismatch".into());
        }
        Ok(a)
    }
    async fn active_orders(&self) -> Result<Value> {
        self.get(
            "accountActiveOrders",
            vec![("account_index", self.0.account.clone())],
            true,
        )
        .await
    }
    async fn inventory(&self) -> Result<account::Inventory> {
        let a = self.account().await?;
        let orders = self.active_orders().await?;
        parse_inventory(&a, &orders, &self.scope())
    }
    /// Read-only contract check against recent historical orders, never resubmits them.
    pub async fn validate_recent_orders(&self) -> Result<usize> {
        let v = self
            .get(
                "accountInactiveOrders",
                vec![
                    ("account_index", self.0.account.clone()),
                    ("market_id", self.0.spec.asset_id.unwrap().to_string()),
                    ("limit", "10".into()),
                ],
                true,
            )
            .await?;
        let rows = v["orders"].as_array().ok_or("history orders missing")?;
        let mut checked = false;
        for row in rows {
            let f = decode_order(
                row,
                &self.scope(),
                self.0.spec.key.symbol(),
                self.0.spec.asset_id.unwrap() as u64,
            )?;
            if !checked && f.client_id.as_deref() != Some("0") {
                let request = Request {
                    scope: f.scope.clone(),
                    symbol: f.order.key.symbol.clone(),
                    side: f.side.clone(),
                    original: f.order.original,
                    selector: Selector::ClientId(
                        f.client_id.clone().ok_or("history client identity")?,
                    ),
                };
                let Outcome::Found(exact) = self.lookup(&request).await? else {
                    return Err("historical exact client lookup unavailable".into());
                };
                if exact.order.key != f.order.key || exact.order.cumulative != f.order.cumulative {
                    return Err("history/exact lookup mismatch".into());
                }
                checked = true;
            }
        }
        Ok(rows.len())
    }
    pub async fn audit(
        &self,
        c: &Coordinator,
        expected: Option<account::Inventory>,
    ) -> Result<account::Report> {
        if c.scope() != &self.scope() {
            return Err("Lighter audit scope".into());
        }
        let ticket = c.begin_account_query().await.map_err(|e| e.to_string())?;
        let started_at = Instant::now();
        let a = self.inventory().await?;
        let b = self.inventory().await?;
        if a != b {
            return Err("Lighter inventory changed during reconciliation".into());
        }
        c.apply_account_snapshot(
            ticket,
            account::Snapshot {
                scope: self.scope(),
                inventory: b,
                started_at,
                completed_at: Instant::now(),
                exchange_time_ms: None,
            },
            expected,
        )
        .await
        .map_err(|e| e.to_string())
    }
    pub async fn check_position(&self, c: &Coordinator, symbol: &str) -> Result<account::Report> {
        if c.scope() != &self.scope() || symbol != self.0.spec.key.symbol() {
            return Err("Lighter position scope".into());
        }
        let ticket = c.begin_account_query().await.map_err(|e| e.to_string())?;
        let started_at = Instant::now();
        let a = self.account().await?;
        let inventory = parse_position(&a, symbol)?;
        c.apply_position_snapshot(
            ticket,
            account::Snapshot {
                scope: self.scope(),
                inventory,
                started_at,
                completed_at: Instant::now(),
                exchange_time_ms: None,
            },
            symbol,
        )
        .await
        .map_err(|e| e.to_string())
    }
}
pub fn parse_position(a: &Value, symbol: &str) -> Result<account::Inventory> {
    let mut inv = account::Inventory {
        perp_dexes: std::collections::BTreeSet::from([String::new()]),
        ..Default::default()
    };
    let mut seen = false;
    for p in a["positions"]
        .as_array()
        .ok_or("missing Lighter positions")?
    {
        if p["symbol"].as_str() != Some(symbol) {
            continue;
        }
        if seen {
            return Err("duplicate selected position".into());
        }
        seen = true;
        let q = dec(&p["position"])?;
        if q < D::ZERO {
            return Err("invalid position magnitude".into());
        }
        if q == D::ZERO {
            continue;
        }
        let sign = match p["sign"].as_i64() {
            Some(1) => D::ONE,
            Some(-1) => -D::ONE,
            _ => return Err("invalid position sign".into()),
        };
        inv.positions.insert(
            account::PositionKey {
                symbol: symbol.into(),
                side: "BOTH".into(),
            },
            q * sign,
        );
    }
    inv.for_position(symbol).map_err(|e| e.to_string())
}
fn find_exact(v: &Value, r: &Request, market: u64, id: &str) -> Result<Option<Found>> {
    let rows = v["orders"].as_array().ok_or("missing Lighter order list")?;
    let id: u64 = id.parse().map_err(|_| "client index")?;
    let matching: Vec<_> = rows
        .iter()
        .filter(|row| row["client_order_index"].as_u64() == Some(id))
        .collect();
    if matching.len() > 1 {
        return Err("duplicate Lighter client index in lookup source".into());
    }
    let Some(row) = matching.first() else {
        return Ok(None);
    };
    let f = decode_order(row, &r.scope, &r.symbol, market)?;
    r.verify(&f).map_err(|e| e.to_string())?;
    Ok(Some(f))
}
pub fn parse_inventory(a: &Value, orders: &Value, scope: &Scope) -> Result<account::Inventory> {
    if a["account_index"].as_u64().map(|n| n.to_string()).as_ref() != Some(&scope.account) {
        return Err("inventory account mismatch".into());
    }
    let mut out = account::Inventory {
        perp_dexes: std::collections::BTreeSet::from([String::new()]),
        includes_wallets: true,
        ..Default::default()
    };
    out.wallets.insert("USDC".into(), dec(&a["collateral"])?);
    // Preserve all reported asset fields in the reviewed baseline, including spot/margin
    // balances. Do not infer that an account with zero perp positions has no other holdings.
    for asset in a["assets"].as_array().ok_or("missing Lighter assets")? {
        let symbol = asset["symbol"].as_str().ok_or("asset symbol missing")?;
        for field in ["balance", "locked_balance", "margin_balance"] {
            let value = dec(&asset[field])?;
            if out
                .wallets
                .insert(format!("{symbol}_{field}"), value)
                .is_some()
            {
                return Err("duplicate Lighter asset".into());
            }
        }
    }
    if a["pending_order_count"].as_u64() != Some(0) {
        return Err("Lighter pending orders prevent stable audit".into());
    }
    if a["bo_positions"].as_array().is_some_and(|v| !v.is_empty()) {
        return Err("binary options holdings unsupported in Lighter audit".into());
    }
    for p in a["positions"]
        .as_array()
        .ok_or("missing Lighter positions")?
    {
        let q = dec(&p["position"])?;
        if q < D::ZERO {
            return Err("invalid position magnitude".into());
        }
        if q == D::ZERO {
            continue;
        }
        let sign = match p["sign"].as_i64() {
            Some(1) => D::ONE,
            Some(-1) => -D::ONE,
            _ => return Err("invalid position sign".into()),
        };
        let key = account::PositionKey {
            symbol: p["symbol"].as_str().ok_or("position symbol")?.into(),
            side: "BOTH".into(),
        };
        if out.positions.insert(key, q * sign).is_some() {
            return Err("duplicate position".into());
        }
    }
    let rows = orders["orders"].as_array().ok_or("missing active orders")?;
    if a["total_order_count"].as_u64() != Some(rows.len() as u64) {
        return Err("incomplete Lighter open orders".into());
    }
    for r in rows {
        let symbol = r["market_symbol"]
            .as_str()
            .ok_or("active order symbol missing")?;
        let market = r["market_index"].as_u64().ok_or("active market ID")?;
        let f = decode_order(r, scope, symbol, market)?;
        if f.order.status.terminal() || r["market_kind"] != "perps" {
            return Err("non-perp/terminal active order".into());
        }
        if out
            .open_orders
            .insert(
                f.order.key,
                account::OpenOrder {
                    side: f.side,
                    remaining: f.order.original - f.order.cumulative,
                    price: dec(&r["price"])?,
                },
            )
            .is_some()
        {
            return Err("duplicate open order".into());
        }
    }
    out.validate().map_err(|e| e.to_string())?;
    Ok(out)
}
