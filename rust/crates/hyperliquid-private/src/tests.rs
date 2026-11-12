use crate::{
    client::Client,
    monitor::{self, Policy},
    Error,
};
use futures_util::{SinkExt, StreamExt};
use perp_arb_private_events::{actor, OrderKey};
use serde_json::{json, Value};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::{JoinHandle, JoinSet},
    time::{sleep, timeout},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};
const USER: &str = "0x0000000000000000000000000000000000000001";
fn trade_order() -> perp_arb_private_events::orders::Order {
    use perp_arb_private_events::orders::*;
    Order {
        venue: "hyperliquid".into(),
        network: Network::Mainnet,
        account: USER.into(),
        symbol: "ETH".into(),
        side: "SELL".into(),
        quantity: "0.01".into(),
        limit_price: "2000".into(),
        tif: TimeInForce::Ioc,
        reduce_only: true,
        client_id: format!("0x{:032x}", 1),
        asset: Some(4),
    }
}
#[tokio::test]
async fn trading_key_is_bound_to_account_and_order_cancel_payloads_are_signed() {
    let server = Mock::new(Behavior {
        trading: true,
        ..Behavior::default()
    })
    .await;
    let c = server.client();
    let trade = c
        .trading(crate::signing::Credentials::new(&format!("{:064x}", 1)).unwrap())
        .await
        .unwrap();
    let o = trade_order();
    assert_eq!(trade.submit_limit(&o).await.unwrap().order_id, "7");
    trade.cancel_limit(&o).await.unwrap();
    let mut wrong = o.clone();
    wrong.network = perp_arb_private_events::orders::Network::Testnet;
    assert_eq!(
        trade.submit_limit(&wrong).await.unwrap_err(),
        Error::Configuration
    );
    let req = server.requests.lock().unwrap();
    assert_eq!(req.len(), 3);
    assert_eq!(req[0]["type"], "userRole");
    assert_eq!(req[1]["action"]["orders"][0]["t"]["limit"]["tif"], "Ioc");
    assert_eq!(req[1]["action"]["orders"][0]["c"], o.client_id);
    assert_eq!(req[2]["action"]["cancels"][0]["cloid"], o.client_id);
    assert!(req[1]["nonce"].as_u64() < req[2]["nonce"].as_u64());
}
#[tokio::test]
async fn an_agent_for_another_account_cannot_get_a_trading_handle() {
    let server = Mock::new(Behavior {
        trading: true,
        wrong_agent: true,
        ..Behavior::default()
    })
    .await;
    assert!(matches!(
        server
            .client()
            .trading(crate::signing::Credentials::new(&format!("{:064x}", 1)).unwrap())
            .await,
        Err(Error::Credentials)
    ));
    assert_eq!(server.requests.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn filled_ack_retains_quantity_and_invalid_fill_is_not_accepted_or_retried() {
    for amount in ["0.005", "0.02", "-1"] {
        let server = Mock::new(Behavior {
            trading: true,
            trade_fill: Some(amount.into()),
            ..Behavior::default()
        })
        .await;
        let trade = server
            .client()
            .trading(crate::signing::Credentials::new(&format!("{:064x}", 1)).unwrap())
            .await
            .unwrap();
        let result = trade.submit_limit(&trade_order()).await;
        if amount == "0.005" {
            assert_eq!(result.unwrap().cumulative.as_deref(), Some(amount));
        } else {
            assert_eq!(result.unwrap_err(), Error::Protocol);
        }
        assert_eq!(server.requests.lock().unwrap().len(), 2);
    }
}
#[tokio::test]
async fn unknown_submission_lookup_uses_cloid_string_and_fixed_read_only_route() {
    use perp_arb_private_events::lookup::{Outcome, Request, Selector};
    let cid = "0x00000000000000000000000000000001";
    let server=Mock::new(Behavior{body:Some(json!({"status":"order","order":{"order":{"coin":"ETH","oid":2,"origSz":"10","sz":"8","side":"B","cloid":cid},"status":"canceled","statusTimestamp":100}}).to_string()),..Behavior::default()}).await;
    let client = server.client();
    let mut r = Request {
        scope: client.scope(),
        symbol: "ETH".into(),
        side: "BUY".into(),
        original: perp_arb_core::decimal("10").unwrap(),
        selector: Selector::ClientId(cid.into()),
    };
    assert!(matches!(
        client.lookup_submission(&r).await,
        Ok(Outcome::Found(_))
    ));
    {
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0]["oid"], cid);
        assert_eq!(requests[0]["type"], "orderStatus");
    }
    r.scope.account = "wrong".into();
    assert!(client.lookup_submission(&r).await.is_err());
    assert_eq!(server.requests.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn cloid_unknown_and_http_429_are_not_terminal_fills() {
    use perp_arb_private_events::lookup::{Outcome, Request, Selector};
    for status in [200, 429] {
        let server = Mock::new(Behavior {
            unknown: true,
            http_status: status,
            ..Behavior::default()
        })
        .await;
        let client = server.client();
        let r = Request {
            scope: client.scope(),
            symbol: "ETH".into(),
            side: "BUY".into(),
            original: perp_arb_core::decimal("10").unwrap(),
            selector: Selector::ClientId("0x00000000000000000000000000000001".into()),
        };
        let result = client.lookup_submission(&r).await;
        if status == 200 {
            assert!(matches!(result, Ok(Outcome::NotFound)));
        } else {
            assert_eq!(result.unwrap_err(), Error::Http(429));
            assert!(
                timeout(Duration::from_millis(80), client.lookup_submission(&r))
                    .await
                    .is_err()
            );
            assert_eq!(server.requests.lock().unwrap().len(), 1);
        }
    }
}
#[tokio::test]
async fn account_snapshot_is_double_sampled_read_only_and_never_adopts_actual_as_expected() {
    let server = Mock::new(Behavior::default()).await;
    let client = server.client();
    let (c, w) = actor::channel(client.scope(), 16, 8).unwrap();
    let task = w.spawn();
    c.connected().await.unwrap();
    let report = client
        .audit_account(&c, &[String::new()], true, None)
        .await
        .unwrap();
    assert!(!report.matches_expected());
    assert!(!report.expected_supplied);
    assert_eq!(server.requests.lock().unwrap().len(), 6);
    assert_eq!(report.inventory().wallets.len(), 1);
    c.shutdown().await.unwrap();
    task.await.unwrap();
}
#[tokio::test]
async fn account_change_between_samples_is_not_reconciled() {
    let server = Mock::new(Behavior {
        account_drift: true,
        ..Behavior::default()
    })
    .await;
    let client = server.client();
    let (c, w) = actor::channel(client.scope(), 16, 8).unwrap();
    let task = w.spawn();
    c.connected().await.unwrap();
    assert_eq!(
        client
            .audit_account(&c, &[String::new()], true, None)
            .await
            .unwrap_err(),
        Error::Protocol
    );
    assert!(c.account_report().await.unwrap().is_none());
    c.shutdown().await.unwrap();
    task.await.unwrap();
}
fn key() -> OrderKey {
    OrderKey {
        symbol: "ETH".into(),
        order_id: "1".into(),
    }
}
fn fill() -> String {
    json!({"channel":"userFills","data":{"user":USER,"fills":[{"coin":"ETH","oid":1,"tid":1,"sz":"2","px":"2000","side":"B","time":100,"hash":format!("0x{:064x}",1)}]}}).to_string()
}
fn order(q: &str) -> Value {
    json!({"order":{"coin":"ETH","oid":1,"origSz":"10","sz":q},"status":"open","statusTimestamp":200})
}
fn ws_order(q: &str) -> String {
    json!({"channel":"orderUpdates","data":[order(q)]}).to_string()
}
fn policy() -> Policy {
    Policy {
        ack_timeout: Duration::from_secs(2),
        heartbeat_every: Duration::from_millis(80),
        heartbeat_timeout: Duration::from_millis(100),
        reconnect_min: Duration::from_millis(10),
        reconnect_max: Duration::from_millis(20),
        scan_every: Duration::from_millis(5),
        poll_every: Duration::from_millis(80),
        recovery_every: Duration::from_millis(20),
        ..Policy::default()
    }
}
struct Mock {
    info: String,
    ws: String,
    requests: Arc<Mutex<Vec<Value>>>,
    messages: Arc<Mutex<Vec<Value>>>,
    tasks: Vec<JoinHandle<()>>,
}
impl Drop for Mock {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}
#[derive(Clone, Default)]
struct Behavior {
    http_delay: u64,
    http_status: u16,
    unknown: bool,
    bad_ack: bool,
    skip_ack: bool,
    skip_snapshot: bool,
    no_pong: bool,
    pre_ack: bool,
    close_first: bool,
    body: Option<String>,
    account_drift: bool,
    trading: bool,
    wrong_agent: bool,
    trade_fill: Option<String>,
}
impl Mock {
    async fn new(b: Behavior) -> Self {
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let info = format!("http://{}/info", http.local_addr().unwrap());
        let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws = format!("ws://{}", ws_listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let messages = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let hb = b.clone();
        let http_task = tokio::spawn(async move {
            let mut jobs = JoinSet::new();
            loop {
                tokio::select! {
                    result=http.accept()=>{let (mut stream,_)=result.unwrap();let captured=captured.clone();let b=hb.clone();jobs.spawn(async move{
                        let mut bytes=Vec::new();let mut buf=[0;4096];
                        let (offset,len)=loop{let n=stream.read(&mut buf).await.unwrap();if n==0{return;}bytes.extend_from_slice(&buf[..n]);if let Some(end)=bytes.windows(4).position(|v|v==b"\r\n\r\n"){
                            let headers=String::from_utf8_lossy(&bytes[..end]);assert!(headers.starts_with("POST /info ") || (b.trading && headers.starts_with("POST /exchange ")));
                            let len=headers.lines().find_map(|s|s.to_ascii_lowercase().strip_prefix("content-length:").and_then(|s|s.trim().parse::<usize>().ok())).unwrap();break(end+4,len);
                        }};
                        while bytes.len()<offset+len{let n=stream.read(&mut buf).await.unwrap();if n==0{return;}bytes.extend_from_slice(&buf[..n]);}
                        let v:Value=serde_json::from_slice(&bytes[offset..offset+len]).unwrap();if !b.trading{assert!(matches!(v["type"].as_str(),Some("orderStatus"|"clearinghouseState"|"openOrders"|"spotClearinghouseState")));assert_eq!(v["user"],USER);}let index={let mut r=captured.lock().unwrap();let i=r.len();r.push(v.clone());i};
                        sleep(Duration::from_millis(b.http_delay)).await;
                        let status=if b.http_status==0{200}else{b.http_status};
                        let body=b.body.unwrap_or_else(||if b.trading {match v["type"].as_str(){
                            Some("userRole")=>json!({"role":"agent","data":{"user":if b.wrong_agent {"0x0000000000000000000000000000000000000002"}else{USER}}}).to_string(),
                            _=>{assert!(v["signature"]["r"].as_str().unwrap().starts_with("0x"));match v["action"]["type"].as_str(){Some("order")=>{let status=if let Some(q)=b.trade_fill{json!({"filled":{"oid":7,"totalSz":q,"avgPx":"2000"}})}else{json!({"resting":{"oid":7}})};json!({"status":"ok","response":{"type":"order","data":{"statuses":[status]}}}).to_string()},Some("cancelByCloid")=>json!({"status":"ok","response":{"type":"cancel","data":{"statuses":["success"]}}}).to_string(),_=>panic!("unexpected trade action")}}
                        }}else{match v["type"].as_str().unwrap(){
                            "clearinghouseState"=>json!({"time":1000,"assetPositions":[],"marginSummary":{"totalRawUsd":if b.account_drift && index>=3 {"101"}else{"100"}}}).to_string(),
                            "openOrders"=>"[]".into(),"spotClearinghouseState"=>json!({"balances":[]}).to_string(),
                            _=>if b.unknown{"{\"status\":\"unknownOid\"}".into()}else{json!({"status":"order","order":order("8")}).to_string()}}});
                        let reply=format!("HTTP/1.1 {status} Mock\r\nContent-Length: {}\r\nRetry-After: 1\r\nConnection: close\r\n\r\n{body}",body.len());let _=stream.write_all(reply.as_bytes()).await;
                    });},Some(_)=jobs.join_next(),if !jobs.is_empty()=>(),
                }
            }
        });
        let captured = messages.clone();
        let ws_task = tokio::spawn(async move {
            let mut jobs = JoinSet::new();
            let mut count = 0;
            loop {
                tokio::select! {
                    result=ws_listener.accept()=>{let (stream,_)=result.unwrap();stream.set_nodelay(true).unwrap();count+=1;let number=count;let b=b.clone();let captured=captured.clone();jobs.spawn(async move{
                        let mut socket=accept_async(stream).await.unwrap();let mut subscriptions=0;
                        while let Some(Ok(msg))=socket.next().await{
                            match msg{Message::Text(raw)=>{let v:Value=serde_json::from_str(&raw).unwrap();captured.lock().unwrap().push(v.clone());
                                if v["method"]=="subscribe"{subscriptions+=1;
                                    if b.pre_ack && subscriptions==1 { let _=socket.send(Message::Text(fill().into())).await; }
                                    if !b.skip_ack {
                                        let mut data=v.clone();if b.bad_ack {data["subscription"]["user"]=json!("wrong");}
                                        let _=socket.send(Message::Text(json!({"channel":"subscriptionResponse","data":data}).to_string().into())).await;
                                    }
                                    if subscriptions==2{
                                        if !b.skip_snapshot {let _=socket.send(Message::Text(json!({"channel":"userFills","data":{"user":USER,"isSnapshot":true,"fills":[]}}).to_string().into())).await;}
                                        if !b.pre_ack {let _=socket.send(Message::Text(ws_order("9").into())).await;}
                                        let _=socket.send(Message::Text(fill().into())).await;
                                        let _=socket.send(Message::Text(fill().into())).await;

                                    }
                                    if subscriptions==4 && b.close_first && number==1{let _=socket.close(None).await;return;}
                                }else if v["method"]=="ping" && !b.no_pong{let _=socket.send(Message::Text("{\"channel\":\"pong\"}".into())).await;}
                            },Message::Ping(p)=>{let _=socket.send(Message::Pong(p)).await;},_=>()}
                        }
                    });},Some(_)=jobs.join_next(),if !jobs.is_empty()=>(),
                }
            }
        });
        Self {
            info,
            ws,
            requests,
            messages,
            tasks: vec![http_task, ws_task],
        }
    }
    fn client(&self) -> Client {
        Client::local(USER, self.info.clone(), self.ws.clone()).unwrap()
    }
}
async fn until(mut f: impl FnMut() -> bool) {
    timeout(Duration::from_secs(4), async {
        while !f() {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
fn coordinator(client: &Client) -> (actor::Coordinator, JoinHandle<()>) {
    let (c, w) = actor::channel(client.scope(), 64, 32).unwrap();
    (c, w.spawn())
}

#[tokio::test]
async fn both_subscriptions_unaggregated_fills_rest_and_shutdown() {
    let mock = Mock::new(Behavior::default()).await;
    let client = mock.client();
    let (c, w) = coordinator(&client);
    let m = monitor::start(client, c.clone(), policy()).await.unwrap();
    let stats = m.watch_stats();
    until(|| stats.borrow().queries >= 1 && stats.borrow().pongs >= 1).await;
    assert!(c.health().connected);
    assert_eq!(stats.borrow().new_fills, 1);
    assert_eq!(stats.borrow().duplicate_fills, 1);
    assert_eq!(
        c.hyperliquid_fill_summary(key())
            .await
            .unwrap()
            .unwrap()
            .observed_quantity,
        perp_arb_core::decimal("2").unwrap()
    );
    assert!(mock
        .messages
        .lock()
        .unwrap()
        .iter()
        .any(|v| v["subscription"]["aggregateByTime"] == false));
    m.shutdown().await.unwrap();
    assert!(!c.health().connected);
    c.shutdown().await.unwrap();
    w.await.unwrap();
}
#[tokio::test]
async fn pre_ack_fill_is_retained_and_query_does_not_block_socket() {
    let mock = Mock::new(Behavior {
        pre_ack: true,
        http_delay: 350,
        ..Behavior::default()
    })
    .await;
    let client = mock.client();
    let (c, w) = coordinator(&client);
    let m = monitor::start(client, c.clone(), policy()).await.unwrap();
    let stats = m.watch_stats();
    until(|| {
        stats.borrow().new_fills == 1
            && stats.borrow().duplicate_fills == 2
            && stats.borrow().pongs >= 1
    })
    .await;
    assert_eq!(stats.borrow().queries, 0);
    assert_eq!(c.health().unresolved_fill_orders, 1);
    until(|| stats.borrow().queries >= 1).await;
    m.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
    w.await.unwrap();
}
#[tokio::test]
async fn reconnect_retains_trade_dedup_and_reconciles_again() {
    let mock = Mock::new(Behavior {
        close_first: true,
        ..Behavior::default()
    })
    .await;
    let client = mock.client();
    let (c, w) = coordinator(&client);
    let m = monitor::start(client, c.clone(), policy()).await.unwrap();
    let stats = m.watch_stats();
    until(|| stats.borrow().connections >= 2 && stats.borrow().queries >= 1).await;
    assert_eq!(stats.borrow().new_fills, 1);
    assert!(stats.borrow().duplicate_fills >= 3);
    m.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
    w.await.unwrap();
}
#[tokio::test]
async fn missing_ack_or_snapshot_never_marks_connected_and_preserves_early_fills() {
    for b in [
        Behavior {
            skip_ack: true,
            pre_ack: true,
            ..Behavior::default()
        },
        Behavior {
            skip_snapshot: true,
            pre_ack: true,
            ..Behavior::default()
        },
    ] {
        let mock = Mock::new(b).await;
        let client = mock.client();
        let (c, w) = coordinator(&client);
        let mut p = policy();
        p.ack_timeout = Duration::from_millis(150);
        let m = monitor::start(client, c.clone(), p).await.unwrap();
        let stats = m.watch_stats();
        until(|| stats.borrow().attempts >= 2).await;
        assert_eq!(stats.borrow().connections, 0);
        assert!(!c.health().connected);
        assert_eq!(
            c.hyperliquid_fill_summary(key())
                .await
                .unwrap()
                .unwrap()
                .unique_fills,
            1
        );
        m.shutdown().await.unwrap();
        c.shutdown().await.unwrap();
        w.await.unwrap();
    }
}
#[tokio::test]
async fn mismatched_ack_never_activates() {
    let mock = Mock::new(Behavior {
        bad_ack: true,
        ..Behavior::default()
    })
    .await;
    let client = mock.client();
    let (c, w) = coordinator(&client);
    let m = monitor::start(client, c.clone(), policy()).await.unwrap();
    let stats = m.watch_stats();
    until(|| stats.borrow().attempts >= 2).await;
    assert_eq!(stats.borrow().connections, 0);
    assert!(!c.health().connected);
    m.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
    w.await.unwrap();
}
#[tokio::test]
async fn missing_application_pong_reconnects() {
    let mock = Mock::new(Behavior {
        no_pong: true,
        ..Behavior::default()
    })
    .await;
    let client = mock.client();
    let (c, w) = coordinator(&client);
    let m = monitor::start(client, c.clone(), policy()).await.unwrap();
    let stats = m.watch_stats();
    until(|| stats.borrow().connections >= 2).await;
    assert_eq!(stats.borrow().pongs, 0);
    assert_eq!(stats.borrow().last_error, Some(Error::Timeout));
    m.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
    w.await.unwrap();
}
#[tokio::test]
async fn duplicate_transport_and_wrong_scope_rejected() {
    let mock = Mock::new(Behavior::default()).await;
    let client = mock.client();
    let (c, w) = coordinator(&client);
    let m = monitor::start(client.clone(), c.clone(), policy())
        .await
        .unwrap();
    assert!(monitor::start(client.clone(), c.clone(), policy())
        .await
        .is_err());
    let (other, ow) = actor::channel(
        perp_arb_private_events::Scope {
            venue: "aster".into(),
            account: USER.into(),
        },
        4,
        4,
    )
    .unwrap();
    let ow = ow.spawn();
    assert!(monitor::start(client, other.clone(), policy())
        .await
        .is_err());
    other.shutdown().await.unwrap();
    ow.await.unwrap();
    m.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
    w.await.unwrap();
}
#[tokio::test]
async fn stop_drains_slow_query_instead_of_losing_reply() {
    let mock = Mock::new(Behavior {
        http_delay: 250,
        ..Behavior::default()
    })
    .await;
    let client = mock.client();
    let (c, w) = coordinator(&client);
    let m = monitor::start(client, c.clone(), policy()).await.unwrap();
    until(|| !mock.requests.lock().unwrap().is_empty()).await;
    m.shutdown().await.unwrap();
    assert_eq!(
        c.subscribe(key()).await.unwrap().borrow().cumulative,
        perp_arb_core::decimal("2").unwrap()
    );
    assert!(!c.health().connected);
    c.shutdown().await.unwrap();
    w.await.unwrap();
}
#[tokio::test]
async fn unknown_order_does_not_clear_fill_pending() {
    let mock = Mock::new(Behavior {
        unknown: true,
        ..Behavior::default()
    })
    .await;
    let client = mock.client();
    let (c, w) = coordinator(&client);
    let m = monitor::start(client, c.clone(), policy()).await.unwrap();
    let stats = m.watch_stats();
    until(|| stats.borrow().query_failures >= 1).await;
    assert_eq!(c.health().unresolved_fill_orders, 1);
    assert!(!c.health().ready_for_new_risk());
    assert_eq!(
        c.hyperliquid_fill_summary(key())
            .await
            .unwrap()
            .unwrap()
            .unique_fills,
        1
    );
    m.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
    w.await.unwrap();
}
#[tokio::test]
async fn http_cooldown_is_shared_across_clones() {
    let mock = Mock::new(Behavior {
        http_status: 429,
        ..Behavior::default()
    })
    .await;
    let client = mock.client();
    let (c, w) = coordinator(&client);
    assert_eq!(
        client.reconcile(&c, key()).await.unwrap_err(),
        Error::Http(429)
    );
    assert!(timeout(
        Duration::from_millis(150),
        client.clone().reconcile(&c, key())
    )
    .await
    .is_err());
    assert_eq!(mock.requests.lock().unwrap().len(), 1);
    c.shutdown().await.unwrap();
    w.await.unwrap();
}
#[tokio::test]
async fn malformed_or_oversized_http_never_becomes_zero_fill() {
    for body in ["not json".to_string(), "x".repeat(65_537)] {
        let mock = Mock::new(Behavior {
            body: Some(body),
            ..Behavior::default()
        })
        .await;
        let client = mock.client();
        let (c, w) = coordinator(&client);
        c.register(key(), perp_arb_core::decimal("10").unwrap())
            .await
            .unwrap();
        assert!(client.reconcile(&c, key()).await.is_err());
        assert!(
            c.subscribe(key())
                .await
                .unwrap()
                .borrow()
                .requires_reconciliation
        );
        c.shutdown().await.unwrap();
        w.await.unwrap();
    }
}
#[tokio::test]
async fn drop_stops_and_releases_transport() {
    let mock = Mock::new(Behavior::default()).await;
    let client = mock.client();
    let (c, w) = coordinator(&client);
    let m = monitor::start(client.clone(), c.clone(), policy())
        .await
        .unwrap();
    let stats = m.watch_stats();
    until(|| stats.borrow().connections == 1).await;
    drop(m);
    until(|| stats.borrow().stopped).await;
    assert!(!c.health().connected);
    let m = monitor::start(client, c.clone(), policy()).await.unwrap();
    m.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
    w.await.unwrap();
}
