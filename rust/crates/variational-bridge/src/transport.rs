use crate::{
    wire::{self, Channel, Event, Observation},
    Result,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
};
use tokio_tungstenite::{
    accept_hdr_async_with_config,
    tungstenite::{
        handshake::server::{ErrorResponse, Request, Response},
        protocol::WebSocketConfig,
        Message,
    },
    WebSocketStream,
};

#[derive(Clone, Debug)]
pub struct Config {
    pub extension_id: String,
    pub private_port: u16,
    pub reference_port: u16,
    pub command_port: u16,
    pub queue_capacity: usize,
}
impl Config {
    pub fn new(extension_id: String) -> Self {
        Self {
            extension_id,
            private_port: 8766,
            reference_port: 8767,
            command_port: 8768,
            queue_capacity: 1024,
        }
    }
    fn validate(&self) -> Result<()> {
        if self.extension_id.len() != 32
            || !self
                .extension_id
                .bytes()
                .all(|b| (b'a'..=b'p').contains(&b))
            || !(1..=8192).contains(&self.queue_capacity)
        {
            return Err("invalid Chrome extension ID or queue capacity".into());
        }
        let ports = [self.private_port, self.reference_port, self.command_port];
        for i in 0..ports.len() {
            for j in i + 1..ports.len() {
                if ports[i] != 0 && ports[i] == ports[j] {
                    return Err("bridge ports must be distinct".into());
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub struct Health {
    pub private_connected: bool,
    pub reference_connected: bool,
    pub command_connected: bool,
    pub native_protocol: bool,
    pub native_trading_enabled: bool,
    pub rejected_handshakes: u64,
    pub fault: Option<String>,
}

/// Only these two read-only page operations are representable. In particular
/// a caller cannot pass arbitrary JSON / PLACE_ORDER / a destructive cancel.
#[derive(Debug)]
pub enum ReadRequest {
    NativeInventory {
        account: String,
    },
    NativeRecovery {
        account: String,
        dispatch_request_id: String,
    },
    NativePreflight {
        account: Option<String>,
    },
    NativeLookup {
        account: String,
        order_id: String,
    },
    PreviewPrice {
        symbol: String,
    },
    CheckOpenOrder {
        symbol: String,
        order_id: Option<String>,
    },
}
impl ReadRequest {
    fn message(self, id: &str) -> Result<(Value, &'static str)> {
        match self {
            Self::NativeInventory { account } => Ok((
                json!({"type":"NATIVE_ORDER_REQUEST","protocol":crate::native::PROTOCOL,"requestId":id,"action":"inventory","account":crate::native::account(&account)?}),
                "NATIVE_ORDER_RESULT",
            )),
            Self::NativeRecovery {
                account,
                dispatch_request_id,
            } => {
                if !crate::native::id(&dispatch_request_id) {
                    return Err("invalid dispatch request ID".into());
                }
                Ok((
                    json!({"type":"NATIVE_ORDER_REQUEST","protocol":crate::native::PROTOCOL,
                    "requestId":id,"action":"recover","account":crate::native::account(&account)?,
                    "dispatchRequestId":dispatch_request_id}),
                    "NATIVE_RECOVERY_RESULT",
                ))
            }
            Self::NativePreflight { account } => {
                let account = account.as_deref().map(crate::native::account).transpose()?;
                let mut message = json!({"type":"NATIVE_ORDER_REQUEST","protocol":crate::native::PROTOCOL,
                    "requestId":id,"action":"preflight"});
                if let Some(account) = account {
                    message["account"] = json!(account);
                }
                Ok((message, "NATIVE_ORDER_RESULT"))
            }
            Self::NativeLookup { account, order_id } => {
                if !crate::native::id(&order_id) {
                    return Err("exact RFQ ID required".into());
                }
                Ok((
                    json!({"type":"NATIVE_ORDER_REQUEST","protocol":crate::native::PROTOCOL,
                    "requestId":id,"action":"lookup","account":crate::native::account(&account)?,
                    "order_id":order_id}),
                    "NATIVE_ORDER_RESULT",
                ))
            }
            Self::PreviewPrice { symbol } => Ok((
                json!({"type":"PREVIEW_LIMIT_ORDER_PRICE", "requestId":id,
                "symbol":wire::symbol(&symbol)?}),
                "PRICE_PREVIEW_RESULT",
            )),
            Self::CheckOpenOrder { symbol, order_id } => {
                if order_id.as_ref().is_some_and(|s| {
                    s.is_empty() || s.len() > 128 || s.chars().any(char::is_control)
                }) {
                    return Err("invalid order ID".into());
                }
                Ok((
                    json!({"type":"CANCEL_ORDER", "requestId":id, "symbol":wire::symbol(&symbol)?,
                    "orderId":order_id, "checkOnly":true}),
                    "CANCEL_RESULT",
                ))
            }
        }
    }
}
struct Command {
    message: Value,
    expected: &'static str,
    response: oneshot::Sender<Result<Value>>,
}

struct Shared {
    events: mpsc::Sender<Event>,
    health: watch::Sender<Health>,
    stop: watch::Sender<bool>,
    origin: String,
    start: Instant,
}
impl Shared {
    fn event(
        &self,
        channel: Channel,
        generation: u64,
        captured_ms: Option<i64>,
        observation: Observation,
    ) -> Result<()> {
        self.event_at(
            channel,
            generation,
            captured_ms,
            observation,
            wire::now_ms()?,
            self.elapsed_us(),
        )
    }
    fn elapsed_us(&self) -> u64 {
        self.start
            .elapsed()
            .as_micros()
            .try_into()
            .unwrap_or(u64::MAX)
    }
    fn event_at(
        &self,
        channel: Channel,
        generation: u64,
        captured_ms: Option<i64>,
        observation: Observation,
        received_ms: i64,
        received_elapsed_us: u64,
    ) -> Result<()> {
        let event = Event {
            channel,
            generation,
            captured_ms,
            observation,
            received_ms,
            received_elapsed_us,
            parsed_elapsed_us: self.elapsed_us(),
        };
        if self.events.try_send(event).is_err() {
            self.health.send_modify(|h| {
                h.fault =
                    Some("event consumer stopped or queue overflow; reconciliation required".into())
            });
            self.stop.send_replace(true);
            return Err("event queue unavailable".into());
        }
        Ok(())
    }
    fn connected(&self, channel: Channel, connected: bool) {
        self.health.send_modify(|h| match channel {
            Channel::Private => h.private_connected = connected,
            Channel::Reference => h.reference_connected = connected,
            Channel::Command => {
                h.command_connected = connected;
                if !connected {
                    h.native_protocol = false;
                    h.native_trading_enabled = false;
                }
            }
        });
    }
}

pub struct Bridge {
    pub private_address: SocketAddr,
    pub reference_address: SocketAddr,
    pub command_address: SocketAddr,
    shared: Arc<Shared>,
    commands: mpsc::Sender<Command>,
    sequence: AtomicU64,
    instance: String,
    tasks: Vec<JoinHandle<()>>,
}
impl Bridge {
    pub fn clock_start(&self) -> Instant {
        self.shared.start
    }
    pub async fn bind(config: Config) -> Result<(Self, mpsc::Receiver<Event>)> {
        config.validate()?;
        let mut random = [0u8; 16];
        getrandom::fill(&mut random).map_err(|_| "request namespace generation failed")?;
        let instance: String = random.iter().map(|b| format!("{b:02x}")).collect();
        // Acquire all three ports before spawning anything. Do not replace or
        // interfere with a running Python broker or another bridge instance.
        let private = TcpListener::bind((Ipv4Addr::LOCALHOST, config.private_port))
            .await
            .map_err(|_| "private port unavailable")?;
        let reference = TcpListener::bind((Ipv4Addr::LOCALHOST, config.reference_port))
            .await
            .map_err(|_| "reference port unavailable")?;
        let command = TcpListener::bind((Ipv4Addr::LOCALHOST, config.command_port))
            .await
            .map_err(|_| "command port unavailable")?;
        let addresses = [
            private.local_addr(),
            reference.local_addr(),
            command.local_addr(),
        ];
        let (events, rx) = mpsc::channel(config.queue_capacity);
        let (health, _) = watch::channel(Health::default());
        let (stop, _) = watch::channel(false);
        let (commands, cmd_rx) = mpsc::channel(1);
        let shared = Arc::new(Shared {
            events,
            health,
            stop,
            origin: format!("chrome-extension://{}", config.extension_id),
            start: Instant::now(),
        });
        let tasks = vec![
            tokio::spawn(serve(private, Channel::Private, shared.clone(), None)),
            tokio::spawn(serve(reference, Channel::Reference, shared.clone(), None)),
            tokio::spawn(serve(
                command,
                Channel::Command,
                shared.clone(),
                Some(cmd_rx),
            )),
        ];
        Ok((
            Self {
                private_address: addresses[0]
                    .as_ref()
                    .map_err(|_| "local address failed")?
                    .to_owned(),
                reference_address: addresses[1]
                    .as_ref()
                    .map_err(|_| "local address failed")?
                    .to_owned(),
                command_address: addresses[2]
                    .as_ref()
                    .map_err(|_| "local address failed")?
                    .to_owned(),
                shared,
                commands,
                sequence: AtomicU64::new(1),
                instance,
                tasks,
            },
            rx,
        ))
    }
    pub fn health(&self) -> watch::Receiver<Health> {
        self.shared.health.subscribe()
    }
    pub async fn request(&self, request: ReadRequest) -> Result<Value> {
        if *self.shared.stop.borrow() || !self.shared.health.borrow().command_connected {
            return Err("extension command channel not ready".into());
        }
        let sequence = self
            .sequence
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |v| v.checked_add(1))
            .map_err(|_| "request sequence exhausted")?;
        let id = format!("rust-read-{}-{sequence}", self.instance);
        let (message, expected) = request.message(&id)?;
        if expected.starts_with("NATIVE_") && !self.shared.health.borrow().native_protocol {
            return Err("reload extension: variational-native-v1 is required".into());
        }
        self.exchange(message, expected).await
    }
    async fn exchange(&self, message: Value, expected: &'static str) -> Result<Value> {
        let (response, reply) = oneshot::channel();
        // No queued automatic replays after disconnect. Busy page commands are
        // rejected rather than collecting a backlog behind slow browser work.
        self.commands
            .try_send(Command {
                message,
                expected,
                response,
            })
            .map_err(|_| "extension command busy")?;
        tokio::time::timeout(Duration::from_secs(67), reply)
            .await
            .map_err(|_| "read request timeout")?
            .map_err(|_| "extension disconnected during read")?
    }
    /// Exact browser-session GET sources; UI symbol matching is never used.
    pub async fn native_query(
        &self,
        account: Option<String>,
        order_id: Option<String>,
    ) -> Result<crate::native::Query> {
        let request = if let Some(order_id) = &order_id {
            ReadRequest::NativeLookup {
                account: account.clone().ok_or("expected account required")?,
                order_id: order_id.clone(),
            }
        } else {
            ReadRequest::NativePreflight {
                account: account.clone(),
            }
        };
        let value = self.request(request).await?;
        let expected = account.as_deref().map(crate::native::account).transpose()?;
        let receipt = crate::native::Receipt::parse(&value, expected.as_deref(), wire::now_ms()?)?;
        crate::native::Query::new(receipt, order_id)
    }
    pub async fn inventory(&self, account: String) -> Result<crate::inventory::Query> {
        let account = crate::native::account(&account)?;
        let started = std::time::Instant::now();
        let value = self
            .request(ReadRequest::NativeInventory {
                account: account.clone(),
            })
            .await?;
        let receipt = crate::native::Receipt::parse(&value, Some(&account), wire::now_ms()?)?;
        crate::inventory::Query::new(receipt, started)
    }
    /// Populate the same Limit/Mid/quantity UI as Python, without clicking Submit.
    pub async fn prepare_maker_ui(
        &self,
        command: &perp_arb_execution::durable::browser::Command,
        instrument: &crate::native::Instrument,
    ) -> Result<String> {
        instrument.validate(&command.symbol)?;
        let message = json!({"type":"NATIVE_ORDER_REQUEST","protocol":crate::native::PROTOCOL,
            "requestId":format!("{}-prepare",command.request_id),"dispatchRequestId":command.request_id,
            "action":"prepare_ui","transport":"ui","account":command.account,
            "order":{"instrument":instrument,"side":command.side.to_ascii_lowercase(),
                "qty":command.quantity,"is_reduce_only":command.reduce_only}});
        let value = self.exchange(message, "NATIVE_ORDER_RESULT").await?;
        let receipt =
            crate::native::Receipt::parse(&value, Some(&command.account), wire::now_ms()?)?;
        if receipt.dispatched || value["prepare_id"] != command.request_id {
            return Err("UI prepare identity mismatch".into());
        }
        let price = value["price"].as_str().ok_or("UI prepared price missing")?;
        if perp_arb_core::decimal(price).map_err(|_| "invalid UI price")?
            <= perp_arb_core::Decimal::ZERO
        {
            return Err("invalid UI price".into());
        }
        Ok(price.into())
    }
    pub async fn recover_native(
        &self,
        account: String,
        dispatch_request_id: String,
    ) -> Result<crate::native::Recovery> {
        let account = crate::native::account(&account)?;
        let value = self
            .request(ReadRequest::NativeRecovery {
                account: account.clone(),
                dispatch_request_id: dispatch_request_id.clone(),
            })
            .await?;
        crate::native::Recovery::parse(&value, &account, &dispatch_request_id, wire::now_ms()?)
    }
    /// Library-only one-shot dispatch. The CLI deliberately does not enable
    /// paired live trading until account/hedge recovery is connected. Any Err
    /// after consuming this permit leaves the durable intent UNKNOWN.
    pub async fn submit_native(
        &self,
        permit: perp_arb_execution::durable::browser::DispatchPermit,
        instrument: crate::native::Instrument,
    ) -> Result<crate::native::Submission> {
        let command = permit.command().clone();
        instrument.validate(&command.symbol)?;
        let account = crate::native::account(&command.account)?;
        if account != command.account {
            return Err("ledger account must be canonical lowercase".into());
        }
        {
            let health = self.shared.health.borrow();
            if *self.shared.stop.borrow()
                || !health.command_connected
                || !health.native_protocol
                || !health.native_trading_enabled
            {
                return Err(
                    "native trading disabled or extension unavailable; reconcile consumed intent"
                        .into(),
                );
            }
        }
        let message = json!({"type":"NATIVE_ORDER_REQUEST","protocol":crate::native::PROTOCOL,
            "requestId":command.request_id,"action":"submit","transport":"ui","account":account,
            "order":{"instrument":instrument,"order_type":"limit","qty":command.quantity,
                "side":command.side.to_ascii_lowercase(),"limit_price":command.limit_price,
                "is_reduce_only":command.reduce_only,"is_auto_resize":false,"use_mark_price":false,"slippage_limit":"0"}});
        let value = self.exchange(message, "NATIVE_ORDER_RESULT").await?;
        let receipt = crate::native::Receipt::parse(&value, Some(&account), wire::now_ms()?)?;
        Ok(crate::native::Submission { receipt, command })
    }
    /// An HTTP cancel acknowledgement never confirms terminal/zero fill.
    /// The caller must keep the WAL cancel pending until exact reconciliation.
    pub async fn cancel_native(
        &self,
        permit: perp_arb_execution::durable::browser::CancelPermit,
    ) -> Result<crate::native::Receipt> {
        let account = crate::native::account(&permit.command().account)?;
        {
            let h = self.shared.health.borrow();
            if *self.shared.stop.borrow()
                || !h.command_connected
                || !h.native_protocol
                || !h.native_trading_enabled
            {
                return Err("native cancel unavailable; keep cancellation unresolved".into());
            }
        }
        let sequence = self
            .sequence
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |v| v.checked_add(1))
            .map_err(|_| "request sequence exhausted")?;
        let message = json!({"type":"NATIVE_ORDER_REQUEST","protocol":crate::native::PROTOCOL,
            "requestId":format!("rust-cancel-{}-{sequence}",self.instance),"action":"cancel",
            "account":account,"order_id":permit.order_id(),"transport":"ui",
            "order":{"instrument":{"underlying":permit.command().symbol},"side":permit.command().side.to_ascii_lowercase(),"qty":permit.command().quantity}});
        let value = self.exchange(message, "NATIVE_ORDER_RESULT").await?;
        let receipt = crate::native::Receipt::parse(&value, Some(&account), wire::now_ms()?)?;
        if !receipt.dispatched
            || receipt.rfq_id.as_deref() != Some(permit.order_id())
            || receipt.source.as_deref() != Some("https://omni.variational.io/api/orders/cancel")
        {
            return Err("cancel receipt identity mismatch".into());
        }
        Ok(receipt)
    }
    pub async fn shutdown(mut self) {
        self.shared.stop.send_replace(true);
        for task in self.tasks.drain(..) {
            let _ = task.await;
        }
    }
}
impl Drop for Bridge {
    fn drop(&mut self) {
        self.shared.stop.send_replace(true);
        self.shared.health.send_modify(|h| {
            h.private_connected = false;
            h.reference_connected = false;
            h.command_connected = false;
        });
        for task in &self.tasks {
            task.abort();
        }
    }
}

// The callback's concrete error response is required by tungstenite's API.
#[allow(clippy::result_large_err)]
async fn handshake(stream: TcpStream, origin: String) -> Result<WebSocketStream<TcpStream>> {
    let callback = move |request: &Request,
                         response: Response|
          -> std::result::Result<Response, ErrorResponse> {
        if request
            .headers()
            .get("origin")
            .and_then(|v| v.to_str().ok())
            != Some(origin.as_str())
        {
            let mut error = ErrorResponse::new(Some("extension origin rejected".into()));
            *error.status_mut() = tokio_tungstenite::tungstenite::http::StatusCode::FORBIDDEN;
            return Err(error);
        }
        Ok(response)
    };
    let settings = WebSocketConfig::default()
        .max_message_size(Some(wire::MAX_FRAME))
        .max_frame_size(Some(wire::MAX_FRAME));
    tokio::time::timeout(
        Duration::from_secs(5),
        accept_hdr_async_with_config(stream, callback, Some(settings)),
    )
    .await
    .map_err(|_| "extension handshake timeout")?
    .map_err(|_| "extension handshake rejected".into())
}

async fn serve(
    listener: TcpListener,
    channel: Channel,
    shared: Arc<Shared>,
    mut commands: Option<mpsc::Receiver<Command>>,
) {
    let mut stop = shared.stop.subscribe();
    let mut generation = 0u64;
    loop {
        if *stop.borrow() {
            break;
        }
        let accepted = tokio::select! { r=listener.accept()=>r, _=stop.changed()=>break };
        let Ok((stream, _)) = accepted else {
            shared.stop.send_replace(true);
            break;
        };
        let socket = tokio::select! { r=handshake(stream, shared.origin.clone())=>r, _=stop.changed()=>break };
        let Ok(socket) = socket else {
            shared
                .health
                .send_modify(|h| h.rejected_handshakes = h.rejected_handshakes.saturating_add(1));
            continue;
        };
        let Some(next) = generation.checked_add(1) else {
            shared.stop.send_replace(true);
            break;
        };
        generation = next;
        // Exactly one connection owns a channel until it disconnects.
        let work = async {
            if let Some(commands) = commands.as_mut() {
                command_connection(socket, generation, &shared, commands).await
            } else {
                feed_connection(socket, channel, generation, &shared).await
            }
        };
        let result = tokio::select! { r=work=>r, _=stop.changed()=>break };
        shared.connected(channel, false);
        if let Err(reason) = result {
            let _ = shared.event(channel, generation, None, Observation::Gap { reason });
        }
        let _ = shared.event(channel, generation, None, Observation::Disconnected);
        if let Some(commands) = commands.as_mut() {
            while let Ok(command) = commands.try_recv() {
                let _ = command
                    .response
                    .send(Err("extension disconnected; request not replayed".into()));
            }
        }
    }
    shared.connected(channel, false);
}

async fn feed_connection(
    mut socket: WebSocketStream<TcpStream>,
    channel: Channel,
    generation: u64,
    shared: &Shared,
) -> Result<()> {
    shared.connected(channel, true);
    shared.event(channel, generation, None, Observation::Connected)?;
    while let Some(message) = socket.next().await {
        let received_elapsed_us = shared.elapsed_us();
        let received_ms = wire::now_ms()?;
        match message.map_err(|_| "extension transport error")? {
            Message::Text(raw) => match wire::decode(channel, &raw, received_ms) {
                Ok(Some((captured, observation))) => shared.event_at(
                    channel,
                    generation,
                    Some(captured),
                    observation,
                    received_ms,
                    received_elapsed_us,
                )?,
                Ok(None) => (),
                Err(reason) => shared.event_at(
                    channel,
                    generation,
                    None,
                    Observation::Gap { reason },
                    received_ms,
                    received_elapsed_us,
                )?,
            },
            Message::Ping(bytes) => socket
                .send(Message::Pong(bytes))
                .await
                .map_err(|_| "pong failed")?,
            Message::Pong(_) => (),
            Message::Close(_) => break,
            _ => return Err("unexpected binary extension frame".into()),
        }
    }
    Ok(())
}

async fn command_connection(
    mut socket: WebSocketStream<TcpStream>,
    generation: u64,
    shared: &Shared,
    commands: &mut mpsc::Receiver<Command>,
) -> Result<()> {
    let message = tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await
        .map_err(|_| "extension registration timeout")?
        .ok_or("extension disconnected before registration")?
        .map_err(|_| "extension registration failed")?;
    let Message::Text(raw) = message else {
        return Err("text registration required".into());
    };
    let value: Value = serde_json::from_str(&raw).map_err(|_| "invalid registration")?;
    if value["type"] != "REGISTER" || value["role"] != "extension" {
        return Err("extension registration required".into());
    }
    shared.health.send_modify(|h| {
        h.native_protocol = value["nativeProtocol"] == crate::native::PROTOCOL;
        h.native_trading_enabled = h.native_protocol
            && value["nativeTradingEnabled"] == true
            && value["nativeMutationTransport"] == "ui";
    });
    socket
        .send(Message::Text(
            json!({"type":"REGISTER_ACK", "ok":true, "role":"extension"})
                .to_string()
                .into(),
        ))
        .await
        .map_err(|_| "registration ack failed")?;
    shared.connected(Channel::Command, true);
    shared.event(Channel::Command, generation, None, Observation::Connected)?;
    loop {
        tokio::select! {
            message=socket.next()=>match message {
                Some(Ok(Message::Ping(bytes)))=>socket.send(Message::Pong(bytes)).await.map_err(|_| "pong failed")?,
                Some(Ok(Message::Text(_))) | Some(Ok(Message::Pong(_)))=>(), // stale replies never satisfy a new request
                _=>return Ok(()),
            },
            command=commands.recv()=>{
                let Some(command) = command else { return Ok(()); };
                if command.response.is_closed() { continue; }
                let result = tokio::time::timeout(Duration::from_secs(if command.message["transport"] == "ui" { 65 } else { 20 }), exchange_read(&mut socket, &command)).await
                    .map_err(|_| "extension read timeout".to_owned()).and_then(|r| r);
                let failed = result.is_err();
                let _ = command.response.send(result);
                if failed { return Err("read channel requires reconnect; no automatic retry".into()); }
            }
        }
    }
}

async fn exchange_read(
    socket: &mut WebSocketStream<TcpStream>,
    command: &Command,
) -> Result<Value> {
    socket
        .send(Message::Text(command.message.to_string().into()))
        .await
        .map_err(|_| "read dispatch failed")?;
    loop {
        let next = socket
            .next()
            .await
            .ok_or("read disconnected")?
            .map_err(|_| "read receive failed")?;
        match next {
            Message::Text(raw) => {
                let value: Value =
                    serde_json::from_str(&raw).map_err(|_| "invalid read response")?;
                if value["requestId"] != command.message["requestId"] {
                    continue;
                }
                if value["type"] != command.expected || value["ok"] != true {
                    if command.expected.starts_with("NATIVE_") {
                        let code = value["receipt"]["error"]
                            .as_str()
                            .or_else(|| value["error"].as_str())
                            .unwrap_or("");
                        let known = [
                            "native_request_failed",
                            "native_deadline",
                            "native_non_json",
                            "native_account_mismatch",
                            "native_session_changed",
                            "native_login_required",
                            "native_account_unavailable",
                            "native_page_shape",
                            "native_page_count",
                            "native_pagination_changed",
                            "native_row_identity_missing",
                            "native_pending_filter_mismatch",
                            "native_positions_shape",
                            "native_portfolio_shape",
                            "native_inventory_incomplete",
                            "native_response_too_large",
                            "native_dispatch_unknown",
                            "native_unavailable_or_disabled",
                            "native_injection_unknown",
                        ];
                        let http = code
                            .strip_prefix("native_http_")
                            .is_some_and(|s| s.len() == 3 && s.bytes().all(|b| b.is_ascii_digit()));
                        if known.contains(&code)
                            || http
                            || (code.starts_with("native_ui_")
                                && code.len() <= 80
                                && code.bytes().all(|b| {
                                    b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'
                                }))
                        {
                            return Err(format!(
                                "native command failed: {code}; no automatic retry"
                            ));
                        }
                    }
                    return Err("extension read failed or wrong response type".into());
                }
                if command.expected == "NATIVE_RECOVERY_RESULT" {
                    let account = command.message["account"]
                        .as_str()
                        .ok_or("recovery account missing")?;
                    let id = command.message["dispatchRequestId"]
                        .as_str()
                        .ok_or("recovery request ID missing")?;
                    // Validation before handing to the owner; this payload is
                    // never printed or persisted by the diagnostic CLI.
                    return Ok(crate::native::Recovery::parse(
                        &value,
                        account,
                        id,
                        wire::now_ms()?,
                    )?
                    .wire_view());
                }
                if command.expected == "NATIVE_ORDER_RESULT" {
                    let expected = command.message["account"].as_str();
                    let receipt = crate::native::Receipt::parse(
                        &value["receipt"],
                        expected,
                        wire::now_ms()?,
                    )?;
                    return serde_json::to_value(receipt)
                        .map_err(|_| "native receipt serialization failed".into());
                }
                // Only project known read result fields; discard page diagnostics.
                if command.expected == "PRICE_PREVIEW_RESULT" {
                    let price = value["price"]
                        .as_str()
                        .ok_or("preview price must be decimal string")?;
                    if perp_arb_core::decimal(price).map_err(|_| "invalid preview price")?
                        <= perp_arb_core::Decimal::ZERO
                    {
                        return Err("non-positive preview price".into());
                    }
                    return Ok(json!({"price":price,"indicative_only":true}));
                }
                let exists = value
                    .get("exists")
                    .or_else(|| value["details"].get("exists"))
                    .and_then(Value::as_bool)
                    .ok_or("missing open order presence")?;
                // The extension can fall back from ID to symbol row matching.
                // Presence is a UI diagnostic, never exact order identity proof.
                return Ok(
                    json!({"exists":exists,"terminal_proven":false,"order_identity_proven":false}),
                );
            }
            Message::Ping(bytes) => socket
                .send(Message::Pong(bytes))
                .await
                .map_err(|_| "pong failed")?,
            Message::Pong(_) => (),
            _ => return Err("read channel closed".into()),
        }
    }
}
