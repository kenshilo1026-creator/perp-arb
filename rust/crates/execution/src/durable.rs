//! Synchronous single-owner execution journal. Run on a dedicated blocking worker,
//! never the socket reader. No method submits, cancels, or silently repairs a log.
pub mod browser;
pub mod single;
use super::*;
use perp_arb_private_events::{
    account::{Inventory, PositionKey},
    lookup::{Outcome, Request, Selector},
    Status,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::Path,
};

const MAX_RECORD: usize = 1_048_576;
const MAX_LOG: u64 = 128 * 1024 * 1024;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalError {
    Io,
    Locked,
    Corrupt,
    Capacity,
    Poisoned,
    Identity,
    RecoveryRequired,
    AlreadyDispatched,
    InvalidEvidence,
    Engine(Error),
}
impl std::fmt::Display for JournalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for JournalError {}
impl From<Error> for JournalError {
    fn from(e: Error) -> Self {
        Self::Engine(e)
    }
}
type JResult<T> = std::result::Result<T, JournalError>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum Target {
    Maker,
    Hedge(String),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OrderEvidence {
    venue: String,
    account: String,
    symbol: String,
    order_id: String,
    original: String,
    cumulative: String,
    status: String,
    event_ms: Option<u64>,
    gap: bool,
}
impl OrderEvidence {
    fn from_snapshot(s: &OrderSnapshot) -> Self {
        Self {
            venue: s.scope.venue.clone(),
            account: s.scope.account.clone(),
            symbol: s.key.symbol.clone(),
            order_id: s.key.order_id.clone(),
            original: s.original.to_string(),
            cumulative: s.cumulative.to_string(),
            status: s.status.as_str().into(),
            event_ms: s.event_ms,
            gap: s.requires_reconciliation,
        }
    }
    fn snapshot(&self, revision: u64) -> JResult<OrderSnapshot> {
        let status = match self.status.as_str() {
            "PENDING" => Status::Pending,
            "NEW" => Status::New,
            "PARTIALLY_FILLED" => Status::PartiallyFilled,
            "FILLED" => Status::Filled,
            "CANCELED" => Status::Canceled,
            "EXPIRED" => Status::Expired,
            "REJECTED" => Status::Rejected,
            _ => return Err(JournalError::Corrupt),
        };
        let now = Instant::now();
        let s = OrderSnapshot {
            scope: Scope {
                venue: self.venue.clone(),
                account: self.account.clone(),
            },
            key: OrderKey {
                symbol: self.symbol.clone(),
                order_id: self.order_id.clone(),
            },
            original: qty(&self.original)?,
            cumulative: qty(&self.cumulative)?,
            status,
            event_ms: self.event_ms,
            revision,
            requires_reconciliation: self.gap,
            observed_at: now,
            applied_at: now,
        };
        validate(&s).map_err(|_| JournalError::InvalidEvidence)?;
        Ok(s)
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
enum Event {
    HedgeWirePrepared {
        id: String,
        order: perp_arb_private_events::orders::Order,
        max_notional: String,
    },
    BrowserMakerBound {
        request_id: String,
        order_id: String,
    },
    BrowserOrder {
        payload: Value,
    },
    SingleOrder {
        payload: Value,
    },
    Created {
        plan: Value,
        nonce: String,
        maker_inventory: Value,
        hedge_inventory: Value,
    },
    Observed {
        target: Target,
        order: OrderEvidence,
    },
    Reserved {
        id: String,
        quantity: String,
    },
    DispatchStarted {
        id: String,
        client_id: String,
    },
    Acknowledged {
        id: String,
        symbol: String,
        order_id: String,
    },
    Unresolved {
        target: Target,
        reason: String,
    },
    RecoveryFence {
        target: Target,
    },
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    schema: u32,
    sequence: u64,
    event: Event,
}
struct Journal {
    file: File,
    sequence: u64,
    hash: [u8; 32],
    length: u64,
    poisoned: bool,
    #[cfg(test)]
    fail: Option<bool>,
}
fn options() -> OpenOptions {
    let mut o = OpenOptions::new();
    o.read(true).write(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        o.share_mode(0);
    }
    o
}
impl Journal {
    fn create(path: &Path) -> JResult<Self> {
        let file = options()
            .create_new(true)
            .open(path)
            .map_err(|_| JournalError::Io)?;
        file.try_lock().map_err(|_| JournalError::Locked)?;
        Ok(Self {
            file,
            sequence: 0,
            hash: [0; 32],
            length: 0,
            poisoned: false,
            #[cfg(test)]
            fail: None,
        })
    }
    fn open(path: &Path) -> JResult<(Self, Vec<Event>)> {
        let mut file = options().open(path).map_err(|_| JournalError::Locked)?;
        file.try_lock().map_err(|_| JournalError::Locked)?;
        let length = file.metadata().map_err(|_| JournalError::Io)?.len();
        if length == 0 || length > MAX_LOG {
            return Err(JournalError::Corrupt);
        }
        let mut previous = [0u8; 32];
        let mut seq = 0;
        let mut consumed = 0;
        let mut events = Vec::new();
        while consumed < length {
            let mut size = [0; 4];
            file.read_exact(&mut size)
                .map_err(|_| JournalError::Corrupt)?;
            let n = u32::from_le_bytes(size) as usize;
            if n == 0 || n > MAX_RECORD || consumed + 4 + n as u64 + 32 > length {
                return Err(JournalError::Corrupt);
            }
            let mut body = vec![0; n];
            let mut digest = [0; 32];
            file.read_exact(&mut body)
                .map_err(|_| JournalError::Corrupt)?;
            file.read_exact(&mut digest)
                .map_err(|_| JournalError::Corrupt)?;
            let mut hasher = Sha256::new();
            hasher.update(previous);
            hasher.update(&body);
            let expected: [u8; 32] = hasher.finalize().into();
            if expected != digest {
                return Err(JournalError::Corrupt);
            }
            let record: Record =
                serde_json::from_slice(&body).map_err(|_| JournalError::Corrupt)?;
            seq += 1;
            if record.schema != 1 || record.sequence != seq {
                return Err(JournalError::Corrupt);
            }
            events.push(record.event);
            previous = digest;
            consumed += 4 + n as u64 + 32;
        }
        Ok((
            Self {
                file,
                sequence: seq,
                hash: previous,
                length,
                poisoned: false,
                #[cfg(test)]
                fail: None,
            },
            events,
        ))
    }
    fn append(&mut self, event: Event) -> JResult<()> {
        if self.poisoned {
            return Err(JournalError::Poisoned);
        }
        let seq = self.sequence.checked_add(1).ok_or(JournalError::Capacity)?;
        let body = serde_json::to_vec(&Record {
            schema: 1,
            sequence: seq,
            event,
        })
        .map_err(|_| JournalError::Corrupt)?;
        let n = body.len();
        if n > MAX_RECORD || self.length + 4 + n as u64 + 32 > MAX_LOG {
            return Err(JournalError::Capacity);
        }
        let mut h = Sha256::new();
        h.update(self.hash);
        h.update(&body);
        let digest: [u8; 32] = h.finalize().into();
        let mut bytes = Vec::with_capacity(n + 36);
        bytes.extend_from_slice(&(n as u32).to_le_bytes());
        bytes.extend_from_slice(&body);
        bytes.extend_from_slice(&digest);
        self.poisoned = true;
        #[cfg(test)]
        if let Some(partial) = self.fail.take() {
            self.file
                .write_all(if partial {
                    &bytes[..bytes.len() / 2]
                } else {
                    &bytes
                })
                .map_err(|_| JournalError::Io)?;
            return Err(JournalError::Io);
        }
        self.file
            .write_all(&bytes)
            .and_then(|_| self.file.sync_all())
            .map_err(|_| JournalError::Io)?;
        self.sequence = seq;
        self.hash = digest;
        self.length += bytes.len() as u64;
        self.poisoned = false;
        Ok(())
    }
}
fn qty(s: &str) -> JResult<Decimal> {
    perp_arb_core::decimal(s).map_err(|_| JournalError::Corrupt)
}
fn text<'a>(v: &'a Value, key: &str) -> JResult<&'a str> {
    v.get(key)
        .and_then(Value::as_str)
        .ok_or(JournalError::Corrupt)
}
fn side(s: &str) -> JResult<Side> {
    match s {
        "BUY" => Ok(Side::Buy),
        "SELL" => Ok(Side::Sell),
        _ => Err(JournalError::Corrupt),
    }
}
fn side_text(side: Side) -> &'static str {
    match side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    }
}
fn leg_json(l: &Leg) -> Value {
    json!({"venue":l.scope.venue,"account":l.scope.account,"symbol":l.symbol,"side":side_text(l.side)})
}
fn leg(v: &Value) -> JResult<Leg> {
    Ok(Leg {
        scope: Scope {
            venue: text(v, "venue")?.into(),
            account: text(v, "account")?.into(),
        },
        symbol: text(v, "symbol")?.into(),
        side: side(text(v, "side")?)?,
    })
}
fn plan_json(p: &Plan) -> Value {
    json!({"run_id":p.run_id,"maker":leg_json(&p.maker),"hedge":leg_json(&p.hedge),"maker_order":p.maker_order.order_id,"quantity":p.quantity.to_string(),"step":p.step.to_string(),"minimum":p.minimum.to_string()})
}
fn plan(v: &Value) -> JResult<Plan> {
    let maker = leg(&v["maker"])?;
    Ok(Plan {
        run_id: text(v, "run_id")?.into(),
        maker_order: OrderKey {
            symbol: maker.symbol.clone(),
            order_id: text(v, "maker_order")?.into(),
        },
        maker,
        hedge: leg(&v["hedge"])?,
        quantity: qty(text(v, "quantity")?)?,
        step: qty(text(v, "step")?)?,
        minimum: qty(text(v, "minimum")?)?,
    })
}
fn inventory_json(i: &Inventory) -> Value {
    json!({"perp_dexes":i.perp_dexes,"includes_spot":i.includes_spot,"includes_wallets":i.includes_wallets,
        "positions":i.positions.iter().map(|(k,q)|json!({"symbol":k.symbol,"side":k.side,"quantity":q.to_string()})).collect::<Vec<_>>(),
        "wallets":i.wallets.iter().map(|(k,q)|json!([k,q.to_string()])).collect::<Vec<_>>(),
        "balances":i.balances.iter().map(|(k,b)|json!([k,b.total.to_string(),b.held.to_string()])).collect::<Vec<_>>()})
}
#[derive(Clone)]
struct State {
    engine: Engine,
    nonce: String,
    dispatched: BTreeMap<String, String>,
    watermarks: BTreeMap<Target, u64>,
    maker_baseline: Value,
    hedge_baseline: Value,
    wire_orders: BTreeMap<String, perp_arb_private_events::orders::Order>,
}
impl State {
    fn apply(&mut self, event: &Event, sequence: u64) -> JResult<()> {
        match event {
            Event::HedgeWirePrepared {
                id,
                order,
                max_notional,
            } => {
                order
                    .validate()
                    .map_err(|_| JournalError::InvalidEvidence)?;
                let intent = &self
                    .engine
                    .hedges
                    .get(id)
                    .ok_or(JournalError::Identity)?
                    .intent;
                let leg = &self.engine.plan.hedge;
                let cap = qty(max_notional)?;
                if self.dispatched.contains_key(id)
                    || self.wire_orders.contains_key(id)
                    || order.venue != leg.scope.venue
                    || order.account != leg.scope.account
                    || order.symbol != leg.symbol
                    || order.side != side_text(intent.side)
                    || qty(&order.quantity)? != intent.quantity
                    || order.client_id != self.client_id(id)?
                    || cap <= Decimal::ZERO
                    || perp_arb_core::instruments::ExactAmount::product(
                        qty(&order.quantity)?,
                        qty(&order.limit_price)?,
                    )
                    .and_then(|a| a.compare(cap))
                    .map_err(|_| JournalError::InvalidEvidence)?
                    .is_gt()
                {
                    return Err(JournalError::Identity);
                }
                self.wire_orders.insert(id.clone(), order.clone());
            }
            Event::BrowserMakerBound {
                request_id,
                order_id,
            } => {
                let p = &mut self.engine.plan;
                if p.maker.scope.venue != "variational"
                    || p.maker_order.order_id != format!("intent:{request_id}")
                    || order_id.is_empty()
                    || order_id.len() > 128
                    || order_id.starts_with("intent:")
                    || self.engine.maker != Decimal::ZERO
                    || !self.engine.hedges.is_empty()
                {
                    return Err(JournalError::Identity);
                }
                p.maker_order.order_id = order_id.clone();
            }
            Event::Created { .. } | Event::SingleOrder { .. } | Event::BrowserOrder { .. } => {
                return Err(JournalError::Corrupt)
            }
            Event::Reserved { id, quantity } => {
                let i = self.engine.reserve_hedge()?.ok_or(JournalError::Corrupt)?;
                if i.id != *id || i.quantity != qty(quantity)? {
                    return Err(JournalError::Corrupt);
                }
            }
            Event::DispatchStarted { id, client_id } => {
                if !self.engine.hedges.contains_key(id)
                    || self.dispatched.contains_key(id)
                    || *client_id != self.client_id(id)?
                {
                    return Err(JournalError::Identity);
                }
                self.dispatched.insert(id.clone(), client_id.clone());
            }
            Event::Acknowledged {
                id,
                symbol,
                order_id,
            } => {
                if !self.dispatched.contains_key(id) {
                    return Err(JournalError::Identity);
                }
                self.engine.acknowledge(
                    id,
                    OrderKey {
                        symbol: symbol.clone(),
                        order_id: order_id.clone(),
                    },
                )?;
            }
            Event::Unresolved { .. } | Event::RecoveryFence { .. } => (),
            Event::Observed { target, order } => {
                let mut snapshot = order.snapshot(sequence)?;
                if let Target::Hedge(id) = target {
                    if let Some(h) = self.engine.hedges.get(id) {
                        if h.terminal
                            && snapshot.status.terminal()
                            && snapshot.cumulative == h.cumulative
                        {
                            snapshot.requires_reconciliation = false;
                        }
                    }
                }
                if let Some(t) = snapshot.event_ms {
                    let watermark = self.watermarks.entry(target.clone()).or_default();
                    *watermark = (*watermark).max(t);
                }
                match target {
                    Target::Maker => self.engine.observe_maker(&snapshot)?,
                    Target::Hedge(id) => {
                        if !self.dispatched.contains_key(id) {
                            return Err(JournalError::Identity);
                        }
                        self.engine.observe_hedge(id, &snapshot)?;
                    }
                }
            }
        }
        Ok(())
    }
    fn client_id(&self, id: &str) -> JResult<String> {
        let bytes = serde_json::to_vec(&json!([
            self.nonce,
            self.engine.plan.hedge.scope.venue,
            self.engine.plan.hedge.scope.account,
            id
        ]))
        .map_err(|_| JournalError::Corrupt)?;
        let hash = Sha256::digest(bytes);
        let hex = hash[..16]
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        match self.engine.plan.hedge.scope.venue.as_str() {
            "lighter" => Ok(u64::from_str_radix(&hex[..12], 16)
                .map_err(|_| JournalError::Identity)?
                .max(1)
                .to_string()),
            "aster" => Ok(format!("pa{hex}")),
            "hyperliquid" => Ok(format!("0x{hex}")),
            _ => Err(JournalError::Identity),
        }
    }
}
pub struct LookupTicket {
    instance: [u8; 16],
    sequence: u64,
    target: Target,
    request: Request,
    started_at: Instant,
}
pub struct BrowserLookupTicket {
    instance: [u8; 16],
    sequence: u64,
    started_at: Instant,
}
impl LookupTicket {
    pub fn request(&self) -> &Request {
        &self.request
    }
}
/// This is a one-use local permission to attempt submission, not a fill or broker.
#[derive(Debug)]
pub struct DispatchPermit {
    intent: Intent,
    client_id: String,
    scope: Scope,
    symbol: String,
    wire_order: Option<perp_arb_private_events::orders::Order>,
}
impl DispatchPermit {
    pub fn wire_order(&self) -> Option<&perp_arb_private_events::orders::Order> {
        self.wire_order.as_ref()
    }
    pub fn intent(&self) -> &Intent {
        &self.intent
    }
    pub fn client_id(&self) -> &str {
        &self.client_id
    }
    pub fn scope(&self) -> &Scope {
        &self.scope
    }
    pub fn symbol(&self) -> &str {
        &self.symbol
    }
}
/// Readable after reopening; never grants permission to retransmit a dispatched intent.
#[derive(Debug, Clone)]
pub struct IntentState {
    pub intent: Intent,
    pub client_id: Option<String>,
    pub order: Option<OrderKey>,
    pub cumulative: Decimal,
    pub terminal_confirmed: bool,
}
pub struct DurableEngine {
    journal: Journal,
    state: State,
    instance: [u8; 16],
    recovery_pending: std::collections::BTreeSet<Target>,
    poisoned: bool,
}
fn random() -> JResult<[u8; 16]> {
    let mut value = [0; 16];
    getrandom::fill(&mut value).map_err(|_| JournalError::Io)?;
    Ok(value)
}
impl DurableEngine {
    /// Local intent key used ONLY before submission. It is never sent as a venue
    /// ID and can only be replaced by the exact persisted browser receipt.
    pub fn pending_browser_order(command: &browser::Command) -> JResult<OrderKey> {
        let id = format!("intent:{}", command.request_id);
        if id.len() > 128 {
            return Err(JournalError::Identity);
        }
        Ok(OrderKey {
            symbol: command.symbol.clone(),
            order_id: id,
        })
    }
    pub fn validate_new_browser_risk(
        &self,
        maker: (&Report, &Health),
        hedge: (&Report, &Health),
        price_ok: bool,
        max_age: Duration,
    ) -> JResult<()> {
        if !self
            .state
            .engine
            .plan
            .maker_order
            .order_id
            .starts_with("intent:")
            || !self.recovery_pending.is_empty()
            || self.faulted()
        {
            return Err(JournalError::RecoveryRequired);
        }
        Engine::arm(
            self.state.engine.plan.clone(),
            maker,
            hedge,
            price_ok,
            max_age,
        )?;
        Ok(())
    }
    pub fn bind_browser_maker(&mut self, maker: &browser::Ledger) -> JResult<()> {
        self.validate_browser_binding(maker)?;
        let view = maker.view();
        let Some(id) = view.order_id else {
            return Err(JournalError::RecoveryRequired);
        };
        if self
            .state
            .engine
            .plan
            .maker_order
            .order_id
            .starts_with("intent:")
        {
            self.commit(Event::BrowserMakerBound {
                request_id: view.command.request_id,
                order_id: id,
            })?;
        }
        Ok(())
    }
    pub fn create(
        path: &Path,
        plan: Plan,
        maker: (&Report, &Health),
        hedge: (&Report, &Health),
        price_ok: bool,
        max_age: Duration,
    ) -> JResult<Self> {
        let engine = Engine::arm(plan, maker, hedge, price_ok, max_age)?;
        // Spot now has its own exact lookup adapter, but this journal still
        // accounts for one-way perpetual quantities, not net spot commissions.
        if engine.plan.maker.scope.venue == "binance_spot" {
            return Err(JournalError::Identity);
        }
        if !matches!(
            engine.plan.hedge.scope.venue.as_str(),
            "aster" | "hyperliquid" | "lighter"
        ) {
            return Err(JournalError::Identity);
        }
        let maker_request = Request {
            scope: engine.plan.maker.scope.clone(),
            symbol: engine.plan.maker.symbol.clone(),
            side: side_text(engine.plan.maker.side).into(),
            original: engine.plan.quantity,
            selector: Selector::OrderId(engine.plan.maker_order.order_id.clone()),
        };
        if maker_request.scope.venue == "variational" {
            // Browser order IDs need not be numeric. They are only reconciled
            // through the dedicated exact browser-evidence path below.
            Update {
                key: engine.plan.maker_order.clone(),
                original: engine.plan.quantity,
                cumulative: Decimal::ZERO,
                status: Status::Pending,
                event_ms: None,
            }
            .validate()
            .map_err(|_| JournalError::Identity)?;
        } else {
            maker_request
                .validate()
                .map_err(|_| JournalError::Identity)?;
        }
        // This ledger is one-way perp only; hedge-mode/spot require explicit unit/position-side accounting.
        for (leg, report) in [(&engine.plan.maker, maker.0), (&engine.plan.hedge, hedge.0)] {
            if leg.symbol.starts_with('@')
                || leg.symbol.contains('/')
                || report
                    .inventory()
                    .positions
                    .keys()
                    .any(|k| k.symbol == leg.symbol && k.side != "BOTH")
            {
                return Err(JournalError::Identity);
            }
        }
        let instance = random()?;
        let nonce = random()?
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let mb = inventory_json(maker.0.inventory());
        let hb = inventory_json(hedge.0.inventory());
        let event = Event::Created {
            plan: plan_json(&engine.plan),
            nonce: nonce.clone(),
            maker_inventory: mb.clone(),
            hedge_inventory: hb.clone(),
        };
        let mut journal = Journal::create(path)?;
        journal.append(event)?;
        Ok(Self {
            journal,
            state: State {
                engine,
                nonce,
                dispatched: BTreeMap::new(),
                watermarks: BTreeMap::new(),
                maker_baseline: mb,
                hedge_baseline: hb,
                wire_orders: BTreeMap::new(),
            },
            instance,
            recovery_pending: Default::default(),
            poisoned: false,
        })
    }
    /// Reopening never carries forward account health or process-local revisions.
    /// All previously dispatched orders and the maker need fresh read-only lookup.
    pub fn open(path: &Path, expected_plan: &Plan) -> JResult<Self> {
        let (journal, events) = Journal::open(path)?;
        let (p, nonce, mb, hb) = match events.first() {
            Some(Event::Created {
                plan,
                nonce,
                maker_inventory,
                hedge_inventory,
            }) => (plan, nonce, maker_inventory, hedge_inventory),
            _ => return Err(JournalError::Corrupt),
        };
        if *p != plan_json(expected_plan)
            || nonce.len() != 32
            || !nonce.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(JournalError::Identity);
        }
        let mut state = State {
            engine: Engine::replay_new(plan(p)?)?,
            nonce: nonce.clone(),
            dispatched: BTreeMap::new(),
            watermarks: BTreeMap::new(),
            maker_baseline: mb.clone(),
            hedge_baseline: hb.clone(),
            wire_orders: BTreeMap::new(),
        };
        for (i, event) in events.iter().enumerate().skip(1) {
            if let Err(e) = state.apply(event, i as u64 + 1) {
                if !state.engine.fault || !matches!(e, JournalError::Engine(Error::InvalidEvidence))
                {
                    return Err(e);
                }
            }
        }
        let mut recovery_pending = std::collections::BTreeSet::from([Target::Maker]);
        recovery_pending.extend(state.dispatched.keys().cloned().map(Target::Hedge));
        Ok(Self {
            journal,
            state,
            instance: random()?,
            recovery_pending,
            poisoned: false,
        })
    }
    fn commit(&mut self, event: Event) -> JResult<()> {
        if self.poisoned {
            return Err(JournalError::Poisoned);
        }
        let mut next = self.state.clone();
        let result = next.apply(&event, self.journal.sequence + 1);
        if result.is_err()
            && !(next.engine.fault
                && matches!(result, Err(JournalError::Engine(Error::InvalidEvidence))))
        {
            return result;
        }
        if let Err(e) = self.journal.append(event) {
            self.state = next;
            self.poisoned = true;
            return Err(e);
        }
        self.state = next;
        result
    }
    pub fn exposure(&self) -> JResult<Decimal> {
        Ok(self.state.engine.exposure()?)
    }
    pub fn in_flight(&self) -> JResult<Decimal> {
        Ok(self.state.engine.in_flight()?)
    }
    pub fn recovery_pending(&self) -> Vec<Target> {
        self.recovery_pending.iter().cloned().collect()
    }
    pub fn intents(&self) -> Vec<IntentState> {
        self.state
            .engine
            .hedges
            .iter()
            .map(|(id, h)| IntentState {
                intent: h.intent.clone(),
                client_id: self.state.dispatched.get(id).cloned(),
                order: h.key.clone(),
                cumulative: h.cumulative,
                terminal_confirmed: h.terminal,
            })
            .collect()
    }
    pub fn faulted(&self) -> bool {
        self.poisoned || self.state.engine.fault
    }
    /// The future transport owner must call this on disconnect/gap before accepting recovery.
    /// It invalidates outstanding lookup tickets without discarding known fills.
    pub fn require_reconciliation(&mut self, target: Target) -> JResult<()> {
        if let Target::Hedge(id) = &target {
            if !self.state.dispatched.contains_key(id) {
                return Err(JournalError::Identity);
            }
        }
        self.commit(Event::RecoveryFence {
            target: target.clone(),
        })?;
        self.recovery_pending.insert(target);
        Ok(())
    }
    pub fn reserve_hedge(&mut self) -> JResult<Option<Intent>> {
        if self.poisoned {
            return Err(JournalError::Poisoned);
        }
        if !self.recovery_pending.is_empty() {
            return Err(JournalError::RecoveryRequired);
        }
        let mut candidate = self.state.engine.clone();
        let intent = candidate.reserve_hedge()?;
        if let Some(i) = &intent {
            self.commit(Event::Reserved {
                id: i.id.clone(),
                quantity: i.quantity.to_string(),
            })?;
        }
        Ok(intent)
    }
    // A missing maker terminal is not permission to ignore already confirmed
    // fills. Keep the maker fence visible, but permit only the known obligation.
    // Any unknown hedge still blocks another dispatch and keeps its reservation.
    fn with_browser_obligation<T>(
        &mut self,
        maker: &browser::Ledger,
        f: impl FnOnce(&mut Self) -> JResult<T>,
    ) -> JResult<T> {
        self.validate_browser_binding(maker)?;
        let view = maker.view();
        if view.poisoned
            || view.trade_conflict
            || view.order_id.is_none()
            || view.confirmed_trade_count == 0
        {
            return Err(JournalError::RecoveryRequired);
        }
        let fenced = self.recovery_pending.remove(&Target::Maker);
        let result = f(self);
        if fenced {
            self.recovery_pending.insert(Target::Maker);
        }
        result
    }
    pub fn reserve_browser_obligation(
        &mut self,
        maker: &browser::Ledger,
    ) -> JResult<Option<Intent>> {
        self.with_browser_obligation(maker, |s| s.reserve_hedge())
    }
    pub fn dispatch_browser_obligation(
        &mut self,
        maker: &browser::Ledger,
        id: &str,
        order: perp_arb_private_events::orders::Order,
        cap: Decimal,
    ) -> JResult<DispatchPermit> {
        self.with_browser_obligation(maker, |s| s.prepare_wire_dispatch(id, order, cap))
    }
    pub fn prepare_dispatch(&mut self, id: &str) -> JResult<DispatchPermit> {
        if self.poisoned {
            return Err(JournalError::Poisoned);
        }
        if !self.recovery_pending.is_empty() {
            return Err(JournalError::RecoveryRequired);
        }
        if self.state.engine.fault {
            return Err(JournalError::Engine(Error::Faulted));
        }
        if self.state.dispatched.contains_key(id) {
            return Err(JournalError::AlreadyDispatched);
        }
        let intent = self
            .state
            .engine
            .hedges
            .get(id)
            .ok_or(JournalError::Identity)?
            .intent
            .clone();
        let client_id = self.state.client_id(id)?;
        self.commit(Event::DispatchStarted {
            id: id.into(),
            client_id: client_id.clone(),
        })?;
        Ok(DispatchPermit {
            intent,
            client_id,
            scope: self.state.engine.plan.hedge.scope.clone(),
            symbol: self.state.engine.plan.hedge.symbol.clone(),
            wire_order: self.state.wire_orders.get(id).cloned(),
        })
    }
    /// Persist exact native wire parameters before acquiring dispatch permission.
    /// A crash after this write can resume only with the identical saved order.
    pub fn prepare_wire_dispatch(
        &mut self,
        id: &str,
        mut order: perp_arb_private_events::orders::Order,
        max_notional: Decimal,
    ) -> JResult<DispatchPermit> {
        if self.faulted() || !self.recovery_pending.is_empty() || max_notional <= Decimal::ZERO {
            return Err(JournalError::RecoveryRequired);
        }
        if perp_arb_core::instruments::ExactAmount::product(
            qty(&order.quantity)?,
            qty(&order.limit_price)?,
        )
        .and_then(|a| a.compare(max_notional))
        .map_err(|_| JournalError::InvalidEvidence)?
        .is_gt()
        {
            return Err(JournalError::InvalidEvidence);
        }
        order.client_id = self.state.client_id(id)?;
        if let Some(old) = self.state.wire_orders.get(id) {
            if old != &order {
                return Err(JournalError::Identity);
            }
        } else {
            self.commit(Event::HedgeWirePrepared {
                id: id.into(),
                order,
                max_notional: max_notional.to_string(),
            })?;
        }
        self.prepare_dispatch(id)
    }
    pub fn acknowledge(&mut self, id: &str, key: OrderKey) -> JResult<()> {
        self.commit(Event::Acknowledged {
            id: id.into(),
            symbol: key.symbol,
            order_id: key.order_id,
        })
    }
    /// A private event may add known fills, but cannot alone clear restart recovery.
    pub fn observe(&mut self, target: Target, snapshot: &OrderSnapshot) -> JResult<()> {
        validate(snapshot)?;
        match &target {
            Target::Maker => {
                let p = &self.state.engine.plan;
                if snapshot.scope != p.maker.scope
                    || snapshot.key != p.maker_order
                    || snapshot.original != p.quantity
                {
                    return Err(JournalError::Identity);
                }
            }
            Target::Hedge(id) => {
                let h = self
                    .state
                    .engine
                    .hedges
                    .get(id)
                    .ok_or(JournalError::Identity)?;
                // Unbound private events have no client ID; bind with a verified lookup or ACK first.
                if snapshot.scope != self.state.engine.plan.hedge.scope
                    || h.key.as_ref() != Some(&snapshot.key)
                    || snapshot.original != h.intent.quantity
                {
                    return Err(JournalError::Identity);
                }
            }
        }
        let known = match &target {
            Target::Maker => self.state.engine.maker,
            Target::Hedge(id) => {
                self.state
                    .engine
                    .hedges
                    .get(id)
                    .ok_or(JournalError::Identity)?
                    .cumulative
            }
        };
        // Replayed older private frames cannot erase fills already established by REST.
        if snapshot.cumulative < known {
            return self.commit(Event::Unresolved {
                target,
                reason: "stale_cumulative".into(),
            });
        }
        let mut order = OrderEvidence::from_snapshot(snapshot);
        if self.recovery_pending.contains(&target)
            || order.event_ms.is_none()
            || self
                .state
                .watermarks
                .get(&target)
                .is_some_and(|last| order.event_ms.is_none_or(|t| t < *last))
        {
            order.gap = true;
        }
        self.commit(Event::Observed { target, order })
    }
    /// Bridge a bound browser maker's durable cumulative into the existing hedge
    /// coordinator. The account reports used to arm this engine remain required.
    /// reduce-only routing of hedge submissions belongs to the execution owner.
    pub fn observe_browser_maker(&mut self, maker: &browser::Ledger) -> JResult<()> {
        self.validate_browser_binding(maker)?;
        self.observe(Target::Maker, &maker.maker_snapshot()?)
    }
    /// Verify the two durable files belong to the same bound maker even before
    /// the first fill snapshot is available. Does not relax the account gate.
    pub fn validate_browser_binding(&self, maker: &browser::Ledger) -> JResult<()> {
        let view = maker.view();
        let plan = &self.state.engine.plan;
        if plan.maker.scope.venue != "variational"
            || view.command.side != side_text(plan.maker.side)
            || view.command.account != plan.maker.scope.account
            || view.command.symbol != plan.maker.symbol
            || (plan.maker_order.order_id != format!("intent:{}", view.command.request_id)
                && view.order_id.as_ref() != Some(&plan.maker_order.order_id))
            || qty(&view.command.quantity)? != plan.quantity
            || (!view.dispatched && !plan.maker_order.order_id.starts_with("intent:"))
        {
            return Err(JournalError::Identity);
        }
        Ok(())
    }
    /// Only a fresh exact lookup in the browser ledger clears its recovery
    /// fence. Unknown hedge reservations remain independently fenced.
    pub fn begin_browser_reconciliation(&self) -> JResult<BrowserLookupTicket> {
        if self.poisoned {
            return Err(JournalError::Poisoned);
        }
        if self.state.engine.plan.maker.scope.venue != "variational" {
            return Err(JournalError::Identity);
        }
        Ok(BrowserLookupTicket {
            instance: self.instance,
            sequence: self.journal.sequence,
            started_at: Instant::now(),
        })
    }
    pub fn reconcile_browser_maker(
        &mut self,
        ticket: BrowserLookupTicket,
        maker: &browser::Ledger,
    ) -> JResult<()> {
        if ticket.instance != self.instance {
            return Err(JournalError::Identity);
        }
        if ticket.sequence != self.journal.sequence
            || ticket.started_at.elapsed() > Duration::from_secs(5)
        {
            return Err(JournalError::RecoveryRequired);
        }
        let s = maker.reconciliation_snapshot_after(ticket.started_at)?;
        let p = &self.state.engine.plan;
        if p.maker.scope.venue != "variational"
            || maker.view().command.side != side_text(p.maker.side)
            || s.scope != p.maker.scope
            || s.key != p.maker_order
            || s.original != p.quantity
        {
            return Err(JournalError::Identity);
        }
        if s.cumulative < self.state.engine.maker
            || self
                .state
                .watermarks
                .get(&Target::Maker)
                .is_some_and(|last| s.event_ms.is_none_or(|ms| ms < *last))
        {
            return Err(JournalError::RecoveryRequired);
        }
        self.commit(Event::Observed {
            target: Target::Maker,
            order: OrderEvidence::from_snapshot(&s),
        })?;
        self.recovery_pending.remove(&Target::Maker);
        Ok(())
    }
    pub fn lookup_ticket(&self, target: Target) -> JResult<LookupTicket> {
        if self.poisoned {
            return Err(JournalError::Poisoned);
        }
        let (leg, quantity, selector) = match &target {
            Target::Maker => (
                &self.state.engine.plan.maker,
                self.state.engine.plan.quantity,
                Selector::OrderId(self.state.engine.plan.maker_order.order_id.clone()),
            ),
            Target::Hedge(id) => {
                let h = self
                    .state
                    .engine
                    .hedges
                    .get(id)
                    .ok_or(JournalError::Identity)?;
                let cid = self
                    .state
                    .dispatched
                    .get(id)
                    .ok_or(JournalError::Identity)?;
                (
                    &self.state.engine.plan.hedge,
                    h.intent.quantity,
                    Selector::ClientId(cid.clone()),
                )
            }
        };
        let request = Request {
            scope: leg.scope.clone(),
            symbol: leg.symbol.clone(),
            side: side_text(leg.side).into(),
            original: quantity,
            selector,
        };
        request.validate().map_err(|_| JournalError::Identity)?;
        Ok(LookupTicket {
            instance: self.instance,
            sequence: self.journal.sequence,
            target,
            request,
            started_at: Instant::now(),
        })
    }
    pub fn apply_lookup(&mut self, ticket: LookupTicket, outcome: Outcome) -> JResult<()> {
        if ticket.instance != self.instance {
            return Err(JournalError::Identity);
        }
        match outcome {
            Outcome::NotFound => self.commit(Event::Unresolved {
                target: ticket.target,
                reason: "not_found".into(),
            }),
            Outcome::Found(found) => {
                ticket
                    .request
                    .verify(&found)
                    .map_err(|_| JournalError::Identity)?;
                let known = match &ticket.target {
                    Target::Maker => self.state.engine.maker,
                    Target::Hedge(id) => {
                        self.state
                            .engine
                            .hedges
                            .get(id)
                            .ok_or(JournalError::Identity)?
                            .cumulative
                    }
                };
                if found.order.cumulative < known {
                    return self.commit(Event::Unresolved {
                        target: ticket.target,
                        reason: "stale_cumulative".into(),
                    });
                }
                // Aster WS dispatch time E and REST order updateTime are different clocks.
                // Fence with local durable sequence and cumulative quantity, never compare them.
                let fresh = ticket.started_at.elapsed() <= Duration::from_secs(30)
                    && ticket.sequence == self.journal.sequence
                    && found.order.event_ms.is_some();
                let now = Instant::now();
                let u = found.order;
                let s = OrderSnapshot {
                    scope: found.scope,
                    key: u.key,
                    original: u.original,
                    cumulative: u.cumulative,
                    status: u.status,
                    event_ms: u.event_ms,
                    revision: 0,
                    requires_reconciliation: !fresh,
                    observed_at: now,
                    applied_at: now,
                };
                self.commit(Event::Observed {
                    target: ticket.target.clone(),
                    order: OrderEvidence::from_snapshot(&s),
                })?;
                if fresh {
                    self.recovery_pending.remove(&ticket.target);
                }
                Ok(())
            }
        }
    }
    /// Persisted independent baseline plus signed cumulative strategy fills.
    /// Cash/fees/funding are deliberately not inferred from current exchange balances.
    pub fn expected_positions(&self, target: Target) -> JResult<BTreeMap<PositionKey, Decimal>> {
        let (baseline, leg, filled) = match target {
            Target::Maker => (
                &self.state.maker_baseline,
                &self.state.engine.plan.maker,
                self.state.engine.maker,
            ),
            Target::Hedge(_) => (
                &self.state.hedge_baseline,
                &self.state.engine.plan.hedge,
                self.state.engine.confirmed_hedge()?,
            ),
        };
        let mut positions = BTreeMap::new();
        for row in baseline["positions"]
            .as_array()
            .ok_or(JournalError::Corrupt)?
        {
            let k = PositionKey {
                symbol: text(row, "symbol")?.into(),
                side: text(row, "side")?.into(),
            };
            if positions.insert(k, qty(text(row, "quantity")?)?).is_some() {
                return Err(JournalError::Corrupt);
            }
        }
        let k = PositionKey {
            symbol: leg.symbol.clone(),
            side: "BOTH".into(),
        };
        let old = positions.get(&k).copied().unwrap_or_default();
        let q = match leg.side {
            Side::Buy => add(old, filled)?,
            Side::Sell => sub(old, filled)?,
        };
        if q == Decimal::ZERO {
            positions.remove(&k);
        } else {
            positions.insert(k, q);
        }
        Ok(positions)
    }
}

#[cfg(test)]
mod tests;
