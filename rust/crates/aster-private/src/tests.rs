use crate::{
    auth::{signing_digest, Credentials},
    client::{HttpPolicy, PrivateHttp},
    monitor::{self, Phase, Policy},
    Error,
};
use futures_util::{SinkExt, StreamExt};
use perp_arb_core::decimal;
use perp_arb_private_events::{actor, OrderKey};
use serde_json::{json, Value};
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::{JoinHandle, JoinSet},
    time::{sleep, timeout, Instant},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

const KEY: &str = "0000000000000000000000000000000000000000000000000000000000000001";
const USER: &str = "0x0000000000000000000000000000000000000001";
fn trade_order() -> perp_arb_private_events::orders::Order {
    use perp_arb_private_events::orders::*;
    Order {
        venue: "aster".into(),
        network: Network::Mainnet,
        account: USER.into(),
        symbol: "ETHUSDT".into(),
        side: "BUY".into(),
        quantity: "0.01".into(),
        limit_price: "2000.00".into(),
        tif: TimeInForce::PostOnly,
        reduce_only: true,
        client_id: "pa123".into(),
        asset: None,
    }
}
#[tokio::test]
async fn trade_submit_and_cancel_use_fixed_routes_same_client_id_and_exact_decimal_strings() {
    let server = HttpMock::new(|r, index| {
        assert_eq!(r.path(), "/fapi/v3/order");
        assert!(r.signed().contains("signature="));
        if index == 0 {
            assert_eq!(r.method, "POST");
            let url = reqwest::Url::parse(&format!("http://localhost/?{}", r.body)).unwrap();
            let params: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();
            assert_eq!(params["quantity"], "0.01");
            assert_eq!(params["price"], "2000");
            assert_eq!(params["timeInForce"], "GTX");
            assert_eq!(params["reduceOnly"], "true");
            assert_eq!(params["newClientOrderId"], "pa123");
            assert_eq!(params["positionSide"], "BOTH");
        } else {
            assert_eq!(r.method, "DELETE");
            assert!(r.body.is_empty());
            assert_eq!(r.param("origClientOrderId").as_deref(), Some("pa123"));
        }
        Response::ok(r#"{"orderId":7,"symbol":"ETHUSDT","clientOrderId":"pa123"}"#)
    })
    .await;
    let c = server.client("ws://localhost", http_policy());
    let o = trade_order();
    assert_eq!(c.submit_limit(&o).await.unwrap().order_id, "7");
    c.cancel_limit(&o).await.unwrap();
    assert_eq!(server.requests.lock().unwrap().len(), 2);
}
#[tokio::test]
async fn trade_timeout_or_server_error_is_not_retried_and_wrong_account_never_sent() {
    for delayed in [false, true] {
        let server = HttpMock::new(move |_, _| {
            if delayed {
                Response::ok("{}").delayed(900)
            } else {
                Response::status(503)
            }
        })
        .await;
        let c = server.client("ws://localhost", http_policy());
        let mut o = trade_order();
        o.account = format!("0x{:040x}", 2);
        assert_eq!(c.submit_limit(&o).await.unwrap_err(), Error::Configuration);
        o.account = USER.into();
        assert!(c.submit_limit(&o).await.is_err());
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}
#[tokio::test]
async fn unknown_submission_lookup_uses_signed_client_id_and_never_submits() {
    use perp_arb_private_events::lookup::{Outcome, Request as Lookup, Selector};
    let server=HttpMock::new(|r,_| {
        assert_eq!(r.method,"GET");assert_eq!(r.path(),"/fapi/v3/order");assert!(r.param("signature").is_some());
        assert_eq!(r.param("origClientOrderId").as_deref(),Some("pa123"));assert!(r.param("orderId").is_none());
        Response::ok(json!({"symbol":"ETHUSDT","orderId":2,"origQty":"10","executedQty":"2","status":"CANCELED","updateTime":100,"side":"BUY","clientOrderId":"pa123"}).to_string())
    }).await;
    let client = server.client("ws://localhost", http_policy());
    let mut r = Lookup {
        scope: client.scope(),
        symbol: "ETHUSDT".into(),
        side: "BUY".into(),
        original: decimal("10").unwrap(),
        selector: Selector::ClientId("pa123".into()),
    };
    assert!(matches!(
        client.lookup_submission(&r).await,
        Ok(Outcome::Found(_))
    ));
    r.scope.account = "other".into();
    assert!(client.lookup_submission(&r).await.is_err());
    assert_eq!(server.requests.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn unknown_submission_absence_errors_and_cooldown_remain_read_only() {
    use perp_arb_private_events::lookup::{Outcome, Request as Lookup, Selector};
    let server = HttpMock::new(|r, index| {
        assert_eq!(r.method, "GET");
        match index {
            0 => Response::ok(r#"{"code":-2013,"msg":"Order does not exist"}"#),
            1 => Response {
                status: 400,
                ..Response::ok(r#"{"code":-2013,"msg":"Order does not exist"}"#)
            },
            2 => Response::status(400),
            _ => Response::status(429).header("Retry-After", "1"),
        }
    })
    .await;
    let client = server.client("ws://localhost", http_policy());
    let r = Lookup {
        scope: client.scope(),
        symbol: "ETHUSDT".into(),
        side: "BUY".into(),
        original: decimal("10").unwrap(),
        selector: Selector::ClientId("pa123".into()),
    };
    assert!(matches!(
        client.lookup_submission(&r).await,
        Ok(Outcome::NotFound)
    ));
    assert!(matches!(
        client.lookup_submission(&r).await,
        Ok(Outcome::NotFound)
    ));
    assert_eq!(
        client.lookup_submission(&r).await.unwrap_err(),
        Error::Http(400)
    );
    assert_eq!(
        client.lookup_submission(&r).await.unwrap_err(),
        Error::Http(429)
    );
    assert!(
        timeout(Duration::from_millis(80), client.lookup_submission(&r))
            .await
            .is_err()
    );
    assert_eq!(server.requests.lock().unwrap().len(), 4);
}
#[tokio::test]
async fn account_reads_are_signed_double_sampled_and_share_weighted_quota() {
    let server = HttpMock::new(|r, _| {
        assert_eq!(r.method, "GET");
        assert!(r.param("signature").is_some());
        match r.path() {
            "/fapi/v3/positionRisk" | "/fapi/v3/openOrders" => Response::ok("[]"),
            "/fapi/v3/balance" => Response::ok(r#"[{"asset":"USDT","balance":"100"}]"#),
            _ => panic!("unexpected route"),
        }
    })
    .await;
    let client = server.client("ws://localhost", http_policy());
    let (c, w) = actor::channel(client.scope(), 8, 8).unwrap();
    let task = w.spawn();
    c.connected().await.unwrap();
    let r = client.audit_account(&c, None).await.unwrap();
    assert!(!r.expected_supplied);
    assert!(!r.matches_expected());
    assert_eq!(r.inventory().wallets.len(), 1);
    assert_eq!(server.requests.lock().unwrap().len(), 6);
    // Only 20 weights remain. A second 100-weight audit times out without bypassing quota.
    assert_eq!(
        client.audit_account(&c, None).await.unwrap_err(),
        Error::Timeout
    );
    assert_eq!(server.requests.lock().unwrap().len(), 7);
    c.shutdown().await.unwrap();
    task.await.unwrap();
}
#[tokio::test]
async fn unequal_account_samples_cannot_become_a_recovery_proof() {
    let server = HttpMock::new(|r, index| match r.path() {
        "/fapi/v3/positionRisk" | "/fapi/v3/openOrders" => Response::ok("[]"),
        "/fapi/v3/balance" => Response::ok(
            json!([{"asset":"USDT","balance":if index<3 {"100"} else {"99"}}]).to_string(),
        ),
        _ => panic!("unexpected route"),
    })
    .await;
    let client = server.client("ws://localhost", http_policy());
    let (c, w) = actor::channel(client.scope(), 8, 8).unwrap();
    let task = w.spawn();
    c.connected().await.unwrap();
    assert_eq!(
        client.audit_account(&c, None).await.unwrap_err(),
        Error::InvalidPayload
    );
    assert!(c.account_report().await.unwrap().is_none());
    c.shutdown().await.unwrap();
    task.await.unwrap();
}
fn key(id: &str) -> OrderKey {
    OrderKey {
        symbol: "ETHUSDT".into(),
        order_id: id.into(),
    }
}
fn ws(q: &str, status: &str) -> Message {
    Message::Text(json!({"e":"ORDER_TRADE_UPDATE","E":200,"o":{"s":"ETHUSDT","i":1,"q":"10","z":q,"X":status}}).to_string().into())
}
fn order(q: &str, status: &str, id: &str) -> String {
    json!({"symbol":"ETHUSDT","orderId":id,"origQty":"10","executedQty":q,"status":status,"updateTime":100}).to_string()
}
fn http_policy() -> HttpPolicy {
    HttpPolicy {
        minimum_spacing: Duration::from_millis(1),
        request_timeout: Duration::from_millis(600),
        total_timeout: Duration::from_secs(2),
        ..HttpPolicy::default()
    }
}
fn policy() -> Policy {
    Policy {
        reconnect_min: Duration::from_millis(10),
        reconnect_max: Duration::from_millis(20),
        scan_every: Duration::from_millis(5),
        poll_every: Duration::from_millis(80),
        retry_every: Duration::from_millis(20),
        heartbeat_every: Duration::from_secs(1),
        heartbeat_timeout: Duration::from_millis(250),
        ..Policy::default()
    }
}

#[derive(Clone)]
struct Request {
    method: String,
    target: String,
    body: String,
}
impl Request {
    fn path(&self) -> &str {
        self.target.split('?').next().unwrap()
    }
    fn param(&self, name: &str) -> Option<String> {
        reqwest::Url::parse(&format!("http://localhost{}", self.target))
            .unwrap()
            .query_pairs()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.into_owned())
    }
    fn signed(&self) -> &str {
        if self.method == "GET" || self.method == "DELETE" {
            self.target.split_once('?').unwrap().1
        } else {
            &self.body
        }
    }
}
struct Response {
    status: u16,
    body: String,
    headers: Vec<(String, String)>,
    delay: Duration,
}
impl Response {
    fn ok(body: impl Into<String>) -> Self {
        Self {
            status: 200,
            body: body.into(),
            headers: vec![],
            delay: Duration::ZERO,
        }
    }
    fn delayed(mut self, ms: u64) -> Self {
        self.delay = Duration::from_millis(ms);
        self
    }
    fn status(status: u16) -> Self {
        Self {
            status,
            ..Self::ok("{}")
        }
    }
    fn header(mut self, key: &str, value: &str) -> Self {
        self.headers.push((key.into(), value.into()));
        self
    }
}
fn default_response(request: &Request) -> Response {
    if request.method == "POST" {
        Response::ok(r#"{"listenKey":"test-listen-key"}"#)
    } else if request.method == "PUT" {
        Response::ok("{}")
    } else {
        Response::ok(order(
            "0",
            "NEW",
            request.param("orderId").as_deref().unwrap_or("1"),
        ))
    }
}
struct HttpMock {
    base: String,
    requests: Arc<Mutex<Vec<Request>>>,
    task: JoinHandle<()>,
}
impl Drop for HttpMock {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl HttpMock {
    async fn new(handler: impl Fn(&Request, usize) -> Response + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let handler = Arc::new(handler);
        let task = tokio::spawn(async move {
            let mut jobs = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (mut socket,_) = accepted.unwrap(); socket.set_nodelay(true).unwrap();
                        let captured = captured.clone(); let handler = handler.clone();
                        jobs.spawn(async move {
                            let mut bytes = Vec::new(); let mut block = [0u8;4096];
                            let (header_end, length) = loop {
                                let n = socket.read(&mut block).await.unwrap_or(0); if n == 0 { return; } bytes.extend(&block[..n]);
                                if let Some(pos) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                                    let header = String::from_utf8_lossy(&bytes[..pos]);
                                    let length = header.lines().find_map(|s| s.to_lowercase().strip_prefix("content-length: ").and_then(|n| n.parse::<usize>().ok())).unwrap_or(0);
                                    break (pos+4, length);
                                }
                                assert!(bytes.len() < 65536);
                            };
                            while bytes.len() < header_end+length {
                                let n = socket.read(&mut block).await.unwrap_or(0); if n == 0 { return; } bytes.extend(&block[..n]);
                            }
                            let header = String::from_utf8_lossy(&bytes[..header_end]); let first: Vec<_> = header.lines().next().unwrap().split_whitespace().collect();
                            let request = Request { method: first[0].into(), target: first[1].into(), body: String::from_utf8(bytes[header_end..header_end+length].to_vec()).unwrap() };
                            let index = { let mut requests = captured.lock().unwrap(); let index = requests.len(); requests.push(request.clone()); index };
                            let response = handler(&request,index); sleep(response.delay).await;
                            let mut output = format!("HTTP/1.1 {} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n", response.status, response.body.len());
                            for (k,v) in response.headers { output.push_str(&format!("{k}: {v}\r\n")); }
                            output.push_str("\r\n"); output.push_str(&response.body);
                            let _ = socket.write_all(output.as_bytes()).await;
                        });
                    }
                    Some(result) = jobs.join_next(), if !jobs.is_empty() => { result.unwrap(); }
                }
            }
        });
        Self {
            base,
            requests,
            task,
        }
    }
    fn count(&self, method: &str) -> usize {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.method == method)
            .count()
    }
    fn client(&self, ws: &str, policy: HttpPolicy) -> PrivateHttp {
        PrivateHttp::local(
            Credentials::new(KEY, USER).unwrap(),
            policy,
            self.base.clone(),
            ws.into(),
        )
        .unwrap()
    }
}

struct Script {
    actions: Vec<(u64, Message)>,
    read: bool,
    wrong_pong: bool,
}
impl Script {
    fn frames(actions: Vec<(u64, Message)>) -> Self {
        Self {
            actions,
            read: true,
            wrong_pong: false,
        }
    }
}
struct WsMock {
    base: String,
    connections: Arc<AtomicUsize>,
    pongs: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}
impl Drop for WsMock {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl WsMock {
    async fn new(plan: impl Fn(usize) -> Script + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("ws://{}", listener.local_addr().unwrap());
        let connections = Arc::new(AtomicUsize::new(0));
        let count = connections.clone();
        let pongs = Arc::new(AtomicUsize::new(0));
        let pong_count = pongs.clone();
        let plan = Arc::new(plan);
        let task = tokio::spawn(async move {
            let mut jobs = JoinSet::new();
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        let (socket,_) = result.unwrap(); socket.set_nodelay(true).unwrap();
                        let n = count.fetch_add(1, Ordering::SeqCst); let script = plan(n); let pongs = pong_count.clone();
                        jobs.spawn(async move {
                            let Ok(mut socket) = accept_async(socket).await else { return; };
                            let start = Instant::now(); let mut actions: VecDeque<_> = script.actions.into();
                            loop {
                                let next = actions.front().map_or(start+Duration::from_secs(3600), |(ms,_)| start+Duration::from_millis(*ms));
                                tokio::select! {
                                    _ = tokio::time::sleep_until(next), if !actions.is_empty() => {
                                        let (_,msg) = actions.pop_front().unwrap(); let close = matches!(msg, Message::Close(_));
                                        if socket.send(msg).await.is_err() || close { return; }
                                    }
                                    message = socket.next(), if script.read => {
                                        match message {
                                            Some(Ok(Message::Ping(payload))) => {
                                                let payload = if script.wrong_pong { vec![99].into() } else { payload };
                                                if socket.send(Message::Pong(payload)).await.is_err() { return; }
                                            }
                                            Some(Ok(Message::Pong(_))) => { pongs.fetch_add(1,Ordering::SeqCst); }
                                            Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return,
                                            _ => (),
                                        }
                                    }
                                    else => std::future::pending::<()>().await,
                                }
                            }
                        });
                    }
                    Some(result) = jobs.join_next(), if !jobs.is_empty() => { result.unwrap(); }
                }
            }
        });
        Self {
            base,
            connections,
            pongs,
            task,
        }
    }
}
async fn until(mut condition: impl FnMut() -> bool) {
    timeout(Duration::from_secs(4), async {
        while !condition() {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("condition did not become true");
}
fn coordinator(client: &PrivateHttp) -> (actor::Coordinator, JoinHandle<()>) {
    let (coordinator, worker) = actor::channel(client.scope(), 64, 32).unwrap();
    (coordinator, worker.spawn())
}

#[test]
fn signing_matches_python_digest_signature_and_encoding_vectors() {
    let fixture: Value =
        serde_json::from_str(include_str!("../fixtures/auth-vectors.json")).unwrap();
    let auth = Credentials::new(KEY, USER).unwrap();
    assert_eq!(auth.signer(), fixture["signer"]);
    for row in fixture["vectors"].as_array().unwrap() {
        let params = row["params"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| (p[0].as_str().unwrap().into(), p[1].as_str().unwrap().into()))
            .collect();
        let signed = auth
            .sign_at(params, row["nonce"].as_u64().unwrap())
            .unwrap();
        assert_eq!(
            signed,
            format!(
                "{}&signature={}",
                row["encoded"].as_str().unwrap(),
                row["signature"].as_str().unwrap()
            )
        );
        assert_eq!(
            hex::encode(signing_digest(row["encoded"].as_str().unwrap())),
            row["digest"]
        );
    }
}

#[test]
fn nonces_are_unique_across_instances_and_clock_rollback_fails() {
    let key = format!("{:064x}", 2);
    let a = Arc::new(Credentials::new(&key, USER).unwrap());
    let b = Credentials::new(&key, USER).unwrap();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64;
    let jobs: Vec<_> = (0..16)
        .map(|_| {
            let a = a.clone();
            std::thread::spawn(move || a.next_nonce(now).unwrap())
        })
        .collect();
    let mut values: Vec<_> = jobs.into_iter().map(|j| j.join().unwrap()).collect();
    values.sort();
    values.dedup();
    assert_eq!(values.len(), 16);
    assert!(b.next_nonce(now).unwrap() > *values.last().unwrap());
    assert_eq!(b.next_nonce(now - 60_000_000), Err(Error::Clock));
}

#[test]
fn credentials_and_policies_reject_invalid_values() {
    assert!(Credentials::new("secret-not-a-key", USER).is_err());
    assert!(Credentials::new(KEY, "http://not-an-address").is_err());
    assert!(Credentials::new(&"0".repeat(64), USER).is_err());
    let bad = HttpPolicy {
        minimum_spacing: Duration::ZERO,
        ..http_policy()
    };
    assert!(PrivateHttp::new(Credentials::new(KEY, USER).unwrap(), bad).is_err());
}

#[tokio::test]
async fn only_listenkey_post_put_and_order_get_are_sent_and_signed() {
    let server = HttpMock::new(|r, _| default_response(r)).await;
    let client = server.client("ws://127.0.0.1:1", http_policy());
    let (c, worker) = coordinator(&client);
    c.register(key("9007199254740993"), decimal("10").unwrap())
        .await
        .unwrap();
    client.listen_key().await.unwrap();
    client.renew().await.unwrap();
    client
        .reconcile_order(&c, key("9007199254740993"))
        .await
        .unwrap();
    let requests = server.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 3);
    assert_eq!(
        requests
            .iter()
            .map(|r| (r.method.as_str(), r.path()))
            .collect::<Vec<_>>(),
        vec![
            ("POST", "/fapi/v3/listenKey"),
            ("PUT", "/fapi/v3/listenKey"),
            ("GET", "/fapi/v3/order")
        ]
    );
    for request in requests {
        let (encoded, signature) = request.signed().split_once("&signature=0x").unwrap();
        let bytes = hex::decode(signature).unwrap();
        let sig = k256::ecdsa::Signature::from_slice(&bytes[..64]).unwrap();
        let recovery = k256::ecdsa::RecoveryId::from_byte(bytes[64] - 27).unwrap();
        let public = k256::ecdsa::VerifyingKey::recover_from_prehash(
            &signing_digest(encoded),
            &sig,
            recovery,
        )
        .unwrap();
        assert_eq!(
            format!(
                "0x{}",
                hex::encode(
                    &crate::auth::hash(&public.to_encoded_point(false).as_bytes()[1..])[12..]
                )
            ),
            Credentials::new(KEY, USER).unwrap().signer()
        );
    }
    c.shutdown().await.unwrap();
    worker.await.unwrap();
}

#[tokio::test]
async fn http_errors_and_unknown_orders_preserve_known_fills() {
    for (status, body) in [
        (404, "{}"),
        (500, "{}"),
        (200, r#"{"code":-2013,"msg":"token=do-not-log"}"#),
        (200, "not-json"),
        (200, "{}"),
    ] {
        let server = HttpMock::new(move |_, _| Response {
            status,
            ..Response::ok(body)
        })
        .await;
        let client = server.client("ws://127.0.0.1:1", http_policy());
        let (c, worker) = coordinator(&client);
        let session = c.connected().await.unwrap();
        c.ingest_aster_ws(session, ws("2", "PARTIALLY_FILLED").to_text().unwrap())
            .await
            .unwrap();
        let view = c.subscribe(key("1")).await.unwrap();
        let error = client.reconcile_order(&c, key("1")).await.unwrap_err();
        assert!(!format!("{error:?}").contains("do-not-log"));
        assert_eq!(view.borrow().cumulative, decimal("2").unwrap());
        assert!(view.borrow().requires_reconciliation);
        c.shutdown().await.unwrap();
        worker.await.unwrap();
    }
}

#[tokio::test]
async fn shared_cooldown_blocks_other_operations_without_extra_requests() {
    for status in [429, 418, 503] {
        let server = HttpMock::new(move |r, n| {
            if n == 0 {
                Response::status(status).header("Retry-After", "1")
            } else {
                default_response(r)
            }
        })
        .await;
        let mut p = http_policy();
        p.request_timeout = Duration::from_millis(100);
        p.total_timeout = Duration::from_millis(150);
        let client = server.client("ws://127.0.0.1:1", p);
        assert!(matches!(client.listen_key().await,Err(Error::Http(s)) if s==status));
        assert_eq!(client.clone().renew().await, Err(Error::Timeout));
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn redirects_large_bodies_and_invalid_listen_keys_are_rejected() {
    for response in [
        Response::status(302).header("Location", "http://127.0.0.1:1/secret"),
        Response::ok("x".repeat(70_000)),
        Response::ok(r#"{"listenKey":"../?token=secret"}"#),
        Response::ok("[]"),
    ] {
        let response = Arc::new(Mutex::new(Some(response)));
        let server = HttpMock::new(move |_, _| response.lock().unwrap().take().unwrap()).await;
        let client = server.client("ws://127.0.0.1:1", http_policy());
        assert!(client.listen_key().await.is_err());
        assert_eq!(server.count("POST"), 1);
    }
}

#[tokio::test]
async fn slow_rest_does_not_block_ws_and_merged_fills_are_not_added_twice() {
    let server = HttpMock::new(|r, _| {
        if r.method == "GET" {
            Response::ok(order("10", "FILLED", "1")).delayed(350)
        } else {
            default_response(r)
        }
    })
    .await;
    let ws_server = WsMock::new(|_| {
        Script::frames(vec![
            (10, ws("2", "PARTIALLY_FILLED")),
            (70, ws("4", "PARTIALLY_FILLED")),
        ])
    })
    .await;
    let client = server.client(&ws_server.base, http_policy());
    let (c, worker) = coordinator(&client);
    c.register(key("1"), decimal("10").unwrap()).await.unwrap();
    let view = c.subscribe(key("1")).await.unwrap();
    let monitor = monitor::start(client, c.clone(), policy()).await.unwrap();
    until(|| server.count("GET") > 0).await;
    until(|| view.borrow().cumulative >= decimal("4").unwrap()).await;
    assert_eq!(monitor.stats().query_successes, 0);
    until(|| view.borrow().cumulative == decimal("10").unwrap()).await;
    monitor.shutdown().await.unwrap();
    assert_eq!(view.borrow().cumulative, decimal("10").unwrap());
    c.shutdown().await.unwrap();
    worker.await.unwrap();
}

#[tokio::test]
async fn slow_listenkey_renewal_does_not_block_ws() {
    let server = HttpMock::new(|r, _| {
        if r.method == "PUT" {
            Response::ok("{}").delayed(350)
        } else {
            default_response(r)
        }
    })
    .await;
    let ws_server = WsMock::new(|_| Script::frames(vec![(90, ws("4", "PARTIALLY_FILLED"))])).await;
    let client = server.client(&ws_server.base, http_policy());
    let (c, worker) = coordinator(&client);
    let mut p = policy();
    p.renew_every = Duration::from_millis(25);
    p.lease_valid_for = Duration::from_secs(2);
    let monitor = monitor::start(client, c.clone(), p).await.unwrap();
    until(|| server.count("PUT") > 0).await;
    until(|| monitor.stats().ws_events == 1).await;
    assert_eq!(monitor.stats().renewals, 0);
    assert_eq!(
        c.subscribe(key("1")).await.unwrap().borrow().cumulative,
        decimal("4").unwrap()
    );
    monitor.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
    worker.await.unwrap();
}

#[tokio::test]
async fn reconnection_keeps_fills_and_rest_reconciles_new_session() {
    let server = HttpMock::new(|r, _| {
        if r.method == "GET" {
            Response::ok(order("4", "CANCELED", "1"))
        } else {
            default_response(r)
        }
    })
    .await;
    let ws_server = WsMock::new(|n| {
        if n == 0 {
            Script::frames(vec![
                (5, ws("2", "PARTIALLY_FILLED")),
                (40, Message::Close(None)),
            ])
        } else {
            Script::frames(vec![(5, ws("4", "CANCELED"))])
        }
    })
    .await;
    let client = server.client(&ws_server.base, http_policy());
    let (c, worker) = coordinator(&client);
    let monitor = monitor::start(client, c.clone(), policy()).await.unwrap();
    until(|| monitor.stats().connections >= 2).await;
    until(|| c.health().ready_for_new_risk() && monitor.stats().ws_events >= 2).await;
    assert_eq!(
        c.subscribe(key("1")).await.unwrap().borrow().cumulative,
        decimal("4").unwrap()
    );
    monitor.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
    worker.await.unwrap();
}

#[tokio::test]
async fn listenkey_expiry_reconnects_without_resetting_cumulative_quantity() {
    let server = HttpMock::new(|r, _| default_response(r)).await;
    let ws_server = WsMock::new(|n| {
        if n == 0 {
            Script::frames(vec![
                (5, ws("2", "PARTIALLY_FILLED")),
                (30, Message::Text(r#"{"e":"listenKeyExpired"}"#.into())),
            ])
        } else {
            Script::frames(vec![(5, ws("4", "PARTIALLY_FILLED"))])
        }
    })
    .await;
    let client = server.client(&ws_server.base, http_policy());
    let (c, worker) = coordinator(&client);
    let monitor = monitor::start(client, c.clone(), policy()).await.unwrap();
    until(|| monitor.stats().connections >= 2 && monitor.stats().ws_events >= 2).await;
    assert_eq!(
        c.subscribe(key("1")).await.unwrap().borrow().cumulative,
        decimal("4").unwrap()
    );
    monitor.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
    worker.await.unwrap();
}

#[tokio::test]
async fn missing_and_wrong_heartbeat_pongs_force_reconnect() {
    for wrong in [false, true] {
        let server = HttpMock::new(|r, _| default_response(r)).await;
        let ws_server = WsMock::new(move |_| Script {
            actions: vec![],
            read: wrong,
            wrong_pong: wrong,
        })
        .await;
        let client = server.client(&ws_server.base, http_policy());
        let (c, worker) = coordinator(&client);
        let mut p = policy();
        p.heartbeat_every = Duration::from_millis(30);
        p.heartbeat_timeout = Duration::from_millis(30);
        let monitor = monitor::start(client, c.clone(), p).await.unwrap();
        until(|| monitor.stats().connections >= 2).await;
        assert_eq!(monitor.stats().last_error, Some(Error::Timeout));
        monitor.shutdown().await.unwrap();
        c.shutdown().await.unwrap();
        worker.await.unwrap();
    }
}

#[tokio::test]
async fn server_ping_is_answered_and_successful_renewal_is_counted() {
    let server = HttpMock::new(|r, _| default_response(r)).await;
    let ws_server =
        WsMock::new(|_| Script::frames(vec![(5, Message::Ping(vec![1, 2, 3].into()))])).await;
    let client = server.client(&ws_server.base, http_policy());
    let (c, worker) = coordinator(&client);
    let mut p = policy();
    p.renew_every = Duration::from_millis(30);
    p.lease_valid_for = Duration::from_secs(1);
    let monitor = monitor::start(client, c.clone(), p).await.unwrap();
    until(|| monitor.stats().renewals > 0 && ws_server.pongs.load(Ordering::SeqCst) > 0).await;
    assert_eq!(monitor.stats().connections, 1);
    monitor.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
    worker.await.unwrap();
}

#[tokio::test]
async fn shutdown_drains_inflight_order_reply_and_retains_newly_known_fill() {
    let server = HttpMock::new(|r, _| {
        if r.method == "GET" {
            Response::ok(order("10", "FILLED", "1")).delayed(200)
        } else {
            default_response(r)
        }
    })
    .await;
    let ws_server = WsMock::new(|_| Script::frames(vec![])).await;
    let client = server.client(&ws_server.base, http_policy());
    let (c, worker) = coordinator(&client);
    c.register(key("1"), decimal("10").unwrap()).await.unwrap();
    let view = c.subscribe(key("1")).await.unwrap();
    let monitor = monitor::start(client, c.clone(), policy()).await.unwrap();
    until(|| server.count("GET") > 0).await;
    monitor.shutdown().await.unwrap();
    assert_eq!(view.borrow().cumulative, decimal("10").unwrap());
    assert!(!c.health().connected);
    assert!(view.borrow().requires_reconciliation);
    c.shutdown().await.unwrap();
    worker.await.unwrap();
}

#[tokio::test]
async fn duplicate_transport_and_account_mismatch_are_rejected() {
    let server = HttpMock::new(|r, _| default_response(r)).await;
    let ws_server = WsMock::new(|_| Script::frames(vec![])).await;
    let client = server.client(&ws_server.base, http_policy());
    let (c, worker) = coordinator(&client);
    let monitor = monitor::start(client.clone(), c.clone(), policy())
        .await
        .unwrap();
    assert!(monitor::start(client.clone(), c.clone(), policy())
        .await
        .is_err());
    let other = server.client(&ws_server.base, http_policy());
    assert!(monitor::start(other, c.clone(), policy()).await.is_err());
    let (wrong, wrong_worker) = actor::channel(
        perp_arb_private_events::Scope {
            venue: "aster".into(),
            account: "other".into(),
        },
        8,
        8,
    )
    .unwrap();
    let wrong_worker = wrong_worker.spawn();
    assert!(matches!(
        monitor::start(client.clone(), wrong.clone(), policy()).await,
        Err(Error::Configuration)
    ));
    monitor.shutdown().await.unwrap();
    let next = monitor::start(client, c.clone(), policy()).await.unwrap();
    next.shutdown().await.unwrap();
    wrong.shutdown().await.unwrap();
    wrong_worker.await.unwrap();
    c.shutdown().await.unwrap();
    worker.await.unwrap();
}

#[tokio::test]
async fn dropping_handle_invalidates_connection_and_releases_lease() {
    let server = HttpMock::new(|r, _| default_response(r)).await;
    let ws_server = WsMock::new(|_| Script::frames(vec![])).await;
    let client = server.client(&ws_server.base, http_policy());
    let (c, worker) = coordinator(&client);
    let monitor = monitor::start(client.clone(), c.clone(), policy())
        .await
        .unwrap();
    let stats = monitor.watch_stats();
    until(|| c.health().connected).await;
    drop(monitor);
    until(|| stats.borrow().phase == Phase::Stopped).await;
    assert!(!c.health().connected);
    let next = monitor::start(client, c.clone(), policy()).await.unwrap();
    next.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
    worker.await.unwrap();
}

#[tokio::test]
async fn authentication_error_stops_without_reconnect_storm() {
    let server = HttpMock::new(|_, _| Response::status(401)).await;
    let client = server.client("ws://127.0.0.1:1", http_policy());
    let (c, worker) = coordinator(&client);
    let monitor = monitor::start(client, c.clone(), policy()).await.unwrap();
    until(|| monitor.stats().phase == Phase::Stopped).await;
    assert_eq!(server.count("POST"), 1);
    assert_eq!(monitor.shutdown().await, Err(Error::Http(401)));
    assert!(!c.health().connected);
    c.shutdown().await.unwrap();
    worker.await.unwrap();
}

#[tokio::test]
async fn invalid_ws_frame_stops_with_known_fills_and_safe_diagnostics() {
    let server = HttpMock::new(|r, _| default_response(r)).await;
    let ws_server = WsMock::new(|_| {
        Script::frames(vec![
            (5, ws("2", "PARTIALLY_FILLED")),
            (30, Message::Text("secret=do-not-log".into())),
        ])
    })
    .await;
    let client = server.client(&ws_server.base, http_policy());
    let (c, worker) = coordinator(&client);
    let monitor = monitor::start(client, c.clone(), policy()).await.unwrap();
    until(|| monitor.stats().phase == Phase::Stopped).await;
    assert_eq!(
        c.subscribe(key("1")).await.unwrap().borrow().cumulative,
        decimal("2").unwrap()
    );
    assert!(!format!("{:?}", monitor.stats()).contains("do-not-log"));
    assert!(monitor.shutdown().await.is_err());
    c.shutdown().await.unwrap();
    worker.await.unwrap();
}

#[tokio::test]
async fn failed_query_retries_then_terminal_order_stops_polling() {
    let count = Arc::new(AtomicUsize::new(0));
    let n = count.clone();
    let server = HttpMock::new(move |r, _| {
        if r.method == "GET" {
            if n.fetch_add(1, Ordering::SeqCst) == 0 {
                Response::status(404)
            } else {
                Response::ok(order("10", "FILLED", "1"))
            }
        } else {
            default_response(r)
        }
    })
    .await;
    let ws_server = WsMock::new(|_| Script::frames(vec![])).await;
    let client = server.client(&ws_server.base, http_policy());
    let (c, worker) = coordinator(&client);
    c.register(key("1"), decimal("10").unwrap()).await.unwrap();
    let view = c.subscribe(key("1")).await.unwrap();
    let monitor = monitor::start(client, c.clone(), policy()).await.unwrap();
    until(|| {
        view.borrow().cumulative == decimal("10").unwrap() && !view.borrow().requires_reconciliation
    })
    .await;
    let before = count.load(Ordering::SeqCst);
    sleep(Duration::from_millis(150)).await;
    assert_eq!(count.load(Ordering::SeqCst), before);
    assert!(monitor.stats().query_failures >= 1);
    monitor.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
    worker.await.unwrap();
}

#[tokio::test]
async fn reconnect_attempt_budget_survives_immediate_socket_failures() {
    let server = HttpMock::new(|r, _| default_response(r)).await;
    let ws_server = WsMock::new(|_| Script::frames(vec![(1, Message::Close(None))])).await;
    let client = server.client(&ws_server.base, http_policy());
    let (c, worker) = coordinator(&client);
    let mut p = policy();
    p.reconnect_min = Duration::from_millis(1);
    p.reconnect_max = Duration::from_millis(2);
    let monitor = monitor::start(client, c.clone(), p).await.unwrap();
    until(|| monitor.stats().connections == 5).await;
    sleep(Duration::from_millis(100)).await;
    assert_eq!(monitor.stats().connection_attempts, 5);
    assert_eq!(ws_server.connections.load(Ordering::SeqCst), 5);
    monitor.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
    worker.await.unwrap();
}

#[tokio::test]
async fn session_age_rotates_before_server_disconnect() {
    let server = HttpMock::new(|r, _| default_response(r)).await;
    let ws_server = WsMock::new(|_| Script::frames(vec![])).await;
    let client = server.client(&ws_server.base, http_policy());
    let (c, worker) = coordinator(&client);
    let mut p = policy();
    p.session_max_age = Duration::from_millis(50);
    let monitor = monitor::start(client, c.clone(), p).await.unwrap();
    until(|| monitor.stats().connections >= 2).await;
    monitor.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
    worker.await.unwrap();
}

#[tokio::test]
async fn renew_failure_reconnects_and_preserves_fill() {
    let failures = Arc::new(AtomicUsize::new(0));
    let count = failures.clone();
    let server = HttpMock::new(move |r, _| {
        if r.method == "PUT" && count.fetch_add(1, Ordering::SeqCst) == 0 {
            Response::status(500)
        } else {
            default_response(r)
        }
    })
    .await;
    let ws_server = WsMock::new(|_| Script::frames(vec![(5, ws("2", "PARTIALLY_FILLED"))])).await;
    let client = server.client(&ws_server.base, http_policy());
    let (c, worker) = coordinator(&client);
    let mut p = policy();
    p.renew_every = Duration::from_millis(35);
    p.lease_valid_for = Duration::from_secs(2);
    let monitor = monitor::start(client, c.clone(), p).await.unwrap();
    until(|| monitor.stats().connections >= 2).await;
    assert_eq!(
        c.subscribe(key("1")).await.unwrap().borrow().cumulative,
        decimal("2").unwrap()
    );
    monitor.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
    worker.await.unwrap();
}

#[tokio::test]
async fn lease_deadline_closes_socket_even_while_renew_is_pending() {
    let server = HttpMock::new(|r, _| {
        if r.method == "PUT" {
            Response::ok("{}").delayed(450)
        } else {
            default_response(r)
        }
    })
    .await;
    let ws_server = WsMock::new(|_| Script::frames(vec![])).await;
    let client = server.client(&ws_server.base, http_policy());
    let (c, worker) = coordinator(&client);
    let mut p = policy();
    p.renew_every = Duration::from_millis(20);
    p.lease_valid_for = Duration::from_millis(100);
    let monitor = monitor::start(client, c.clone(), p).await.unwrap();
    until(|| monitor.stats().connections >= 2).await;
    assert_eq!(monitor.stats().renewals, 0);
    assert_eq!(monitor.stats().last_error, Some(Error::Timeout));
    monitor.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
    worker.await.unwrap();
}

#[tokio::test]
async fn ping_flood_is_bounded_and_forces_recovery() {
    let server = HttpMock::new(|r, _| default_response(r)).await;
    let ws_server = WsMock::new(|n| {
        if n == 0 {
            Script::frames(
                (1..=12)
                    .map(|n| (n, Message::Ping(vec![n as u8].into())))
                    .collect(),
            )
        } else {
            Script::frames(vec![])
        }
    })
    .await;
    let client = server.client(&ws_server.base, http_policy());
    let (c, worker) = coordinator(&client);
    let monitor = monitor::start(client, c.clone(), policy()).await.unwrap();
    until(|| monitor.stats().connections >= 2).await;
    assert_eq!(monitor.stats().last_error, Some(Error::WsLimit));
    assert!(ws_server.pongs.load(Ordering::SeqCst) <= 8);
    monitor.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
    worker.await.unwrap();
}

#[tokio::test]
async fn request_timeout_preserves_exposure_and_does_not_retry_in_place() {
    let server = HttpMock::new(|_, _| Response::ok(order("10", "FILLED", "1")).delayed(300)).await;
    let mut p = http_policy();
    p.request_timeout = Duration::from_millis(60);
    p.total_timeout = Duration::from_millis(150);
    let client = server.client("ws://127.0.0.1:1", p);
    let (c, worker) = coordinator(&client);
    let session = c.connected().await.unwrap();
    c.ingest_aster_ws(session, ws("2", "PARTIALLY_FILLED").to_text().unwrap())
        .await
        .unwrap();
    assert_eq!(
        client.reconcile_order(&c, key("1")).await.unwrap_err(),
        Error::Timeout
    );
    let view = c.subscribe(key("1")).await.unwrap();
    assert_eq!(view.borrow().cumulative, decimal("2").unwrap());
    assert!(view.borrow().requires_reconciliation);
    assert_eq!(server.count("GET"), 1);
    c.shutdown().await.unwrap();
    worker.await.unwrap();
}

#[tokio::test]
async fn shared_minute_budget_includes_listenkey_and_queries() {
    let server = HttpMock::new(|r, _| default_response(r)).await;
    let mut p = http_policy();
    p.requests_per_minute = 1;
    p.request_timeout = Duration::from_millis(80);
    p.total_timeout = Duration::from_millis(120);
    let client = server.client("ws://127.0.0.1:1", p);
    let (c, worker) = coordinator(&client);
    c.register(key("1"), decimal("10").unwrap()).await.unwrap();
    client.listen_key().await.unwrap();
    assert_eq!(
        client
            .clone()
            .reconcile_order(&c, key("1"))
            .await
            .unwrap_err(),
        Error::Timeout
    );
    assert_eq!(server.requests.lock().unwrap().len(), 1);
    assert!(
        c.subscribe(key("1"))
            .await
            .unwrap()
            .borrow()
            .requires_reconciliation
    );
    c.shutdown().await.unwrap();
    worker.await.unwrap();
}

#[tokio::test]
async fn multiple_orders_are_polled_fairly_and_terminal_views_stop_polling() {
    let server = HttpMock::new(|r, _| {
        if r.method == "GET" {
            Response::ok(order("10", "FILLED", &r.param("orderId").unwrap())).delayed(25)
        } else {
            default_response(r)
        }
    })
    .await;
    let ws_server = WsMock::new(|_| Script::frames(vec![])).await;
    let client = server.client(&ws_server.base, http_policy());
    let (c, worker) = coordinator(&client);
    let mut views = vec![];
    for id in 1..=6 {
        let key = key(&id.to_string());
        c.register(key.clone(), decimal("10").unwrap())
            .await
            .unwrap();
        views.push(c.subscribe(key).await.unwrap());
    }
    let monitor = monitor::start(client, c.clone(), policy()).await.unwrap();
    until(|| {
        views.iter().all(|v| {
            v.borrow().cumulative == decimal("10").unwrap() && !v.borrow().requires_reconciliation
        })
    })
    .await;
    let before = server.count("GET");
    sleep(Duration::from_millis(150)).await;
    assert_eq!(server.count("GET"), before);
    assert!(before >= 6);
    monitor.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
    worker.await.unwrap();
}

#[tokio::test]
async fn recovery_prioritizes_active_orders_over_retained_terminal_history() {
    let server = HttpMock::new(|r, _| {
        if r.method == "GET" {
            let id = r.param("orderId").unwrap();
            if id == "999" {
                Response::ok(order("2", "PARTIALLY_FILLED", &id))
            } else {
                Response::ok(order("10", "FILLED", &id))
            }
        } else {
            default_response(r)
        }
    })
    .await;
    let ws_server = WsMock::new(|_| Script::frames(vec![])).await;
    let client = server.client(&ws_server.base, http_policy());
    let (c, worker) = coordinator(&client);
    for id in 1..=6 {
        let id = id.to_string();
        c.register(key(&id), decimal("10").unwrap()).await.unwrap();
        let ticket = c.begin_query(key(&id)).await.unwrap();
        c.ingest_aster_rest(ticket, &order("10", "FILLED", &id))
            .await
            .unwrap();
    }
    c.register(key("999"), decimal("10").unwrap())
        .await
        .unwrap();
    let mut p = policy();
    p.query_concurrency = 1;
    let monitor = monitor::start(client, c.clone(), p).await.unwrap();
    until(|| {
        let requests = server.requests.lock().unwrap();
        (1..=6).all(|id| {
            requests
                .iter()
                .any(|r| r.method == "GET" && r.param("orderId") == Some(id.to_string()))
        })
    })
    .await;
    let requests = server.requests.lock().unwrap().clone();
    assert_eq!(
        requests
            .iter()
            .find(|r| r.method == "GET")
            .unwrap()
            .param("orderId")
            .as_deref(),
        Some("999")
    );
    // Spare query slots still reconcile the retained terminal history.
    for id in 1..=6 {
        assert!(requests
            .iter()
            .any(|r| r.method == "GET" && r.param("orderId") == Some(id.to_string())));
    }
    monitor.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
    worker.await.unwrap();
}

#[tokio::test]
async fn stopping_during_listenkey_request_cancels_setup_without_delete() {
    let server = HttpMock::new(|r, _| default_response(r).delayed(500)).await;
    let client = server.client("ws://127.0.0.1:1", http_policy());
    let (c, worker) = coordinator(&client);
    let monitor = monitor::start(client, c.clone(), policy()).await.unwrap();
    until(|| server.count("POST") == 1).await;
    timeout(Duration::from_secs(1), monitor.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(server.count("DELETE"), 0);
    assert!(!c.health().connected);
    c.shutdown().await.unwrap();
    worker.await.unwrap();
}

#[tokio::test]
async fn disconnected_stream_uses_recovery_poll_interval() {
    let server = HttpMock::new(|r, _| {
        if r.method == "POST" {
            Response::status(503)
        } else {
            Response::ok(order("2", "PARTIALLY_FILLED", "1"))
        }
    })
    .await;
    let client = server.client("ws://127.0.0.1:1", http_policy());
    let (c, worker) = coordinator(&client);
    c.register(key("1"), decimal("10").unwrap()).await.unwrap();
    let mut p = policy();
    p.poll_every = Duration::from_secs(10);
    p.recovery_poll_every = Duration::from_millis(20);
    let monitor = monitor::start(client, c.clone(), p).await.unwrap();
    until(|| server.count("GET") >= 2).await;
    assert!(!c.health().connected);
    assert_eq!(
        c.subscribe(key("1")).await.unwrap().borrow().cumulative,
        decimal("2").unwrap()
    );
    monitor.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
    worker.await.unwrap();
}
