use super::multiplex::Pacer;
use super::*;
use crate::metadata::{CachePolicy, PublicHttp, Source};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{timeout, Instant},
};

fn meta(delisted: bool, sz: u32) -> Value {
    json!({"universe":[{"name":"ETH","szDecimals":sz,"isDelisted":delisted},{"name":"BTC","szDecimals":4}]})
}
async fn metadata_cache(
    responses: Vec<(u16, Value)>,
    policy: CachePolicy,
) -> (MetadataCache, tokio::task::JoinHandle<()>) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/meta", l.local_addr().unwrap());
    let task = tokio::spawn(async move {
        for (status, value) in responses {
            let (mut stream, _) = l.accept().await.unwrap();
            let mut raw = Vec::new();
            loop {
                let mut b = [0u8; 1024];
                let n = stream.read(&mut b).await.unwrap();
                if n == 0 {
                    break;
                }
                raw.extend_from_slice(&b[..n]);
                if raw.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            let body = value.to_string();
            let response = format!(
                "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });
    (
        MetadataCache::new(PublicHttp::local_test(&url), Source::Hyperliquid, policy).unwrap(),
        task,
    )
}

#[tokio::test]
async fn metadata_generation_refresh_reconnects_before_reusing_quotes() {
    let (cache, server) = metadata_cache(
        vec![(200, meta(false, 4)), (200, meta(false, 3))],
        CachePolicy::default(),
    )
    .await;
    cache.refresh(false).await.unwrap();
    let (l, url) = listener().await;
    let mut hub = MarketHub::new();
    hub.bind_metadata(cache.clone(), SymbolMappings::default())
        .unwrap();
    let mut eth = hub
        .subscribe_at(key(Venue::Hyperliquid, "ETH"), url, timing())
        .await
        .unwrap();
    let mut s = accept(&l).await;
    subscriptions(&mut s, 2).await;
    hl(&mut s, "ETH", "100").await;
    let first = until(&mut eth, |v| v.connected).await;
    cache.refresh(true).await.unwrap();
    until(&mut eth, |v| !v.connected).await;
    let mut s2 = accept(&l).await;
    subscriptions(&mut s2, 2).await;
    assert!(!eth.snapshot().connected);
    hl(&mut s2, "ETH", "101").await;
    assert!(until(&mut eth, |v| v.connected).await.generation > first.generation);
    assert_eq!(hub.stats(Venue::Hyperliquid).handovers, 1);
    assert!(hub.bind_metadata(cache, SymbolMappings::default()).is_err());
    hub.shutdown().await.unwrap();
    server.await.unwrap();
}
#[tokio::test]
async fn metadata_failure_pauses_then_recovers_and_delisted_coin_stays_off() {
    let (cache, server) = metadata_cache(
        vec![
            (200, meta(false, 4)),
            (400, json!({})),
            (200, meta(true, 4)),
        ],
        CachePolicy {
            failure_retry: Duration::from_millis(10),
            ..CachePolicy::default()
        },
    )
    .await;
    cache.refresh(false).await.unwrap();
    let (l, url) = listener().await;
    let mut hub = MarketHub::new();
    hub.bind_metadata(cache.clone(), SymbolMappings::default())
        .unwrap();
    let mut eth = hub
        .subscribe_at(key(Venue::Hyperliquid, "ETH"), url.clone(), timing())
        .await
        .unwrap();
    let mut btc = hub
        .subscribe_at(key(Venue::Hyperliquid, "BTC"), url, timing())
        .await
        .unwrap();
    let mut s = accept(&l).await;
    subscriptions(&mut s, 4).await;
    hl(&mut s, "ETH", "100").await;
    hl(&mut s, "BTC", "500").await;
    until(&mut eth, |v| v.connected).await;
    until(&mut btc, |v| v.connected).await;
    assert!(cache.refresh(true).await.is_err());
    until(&mut eth, |v| !v.connected).await;
    until(&mut btc, |v| !v.connected).await;
    assert!(timeout(Duration::from_millis(120), l.accept())
        .await
        .is_err());
    cache.refresh(true).await.unwrap();
    let mut s2 = accept(&l).await;
    let cmds = subscriptions(&mut s2, 2).await;
    assert!(cmds.iter().all(|c| c["subscription"]["coin"] == "BTC"));
    hl(&mut s2, "ETH", "999").await;
    hl(&mut s2, "BTC", "501").await;
    until(&mut btc, |v| v.connected).await;
    assert!(!eth.snapshot().connected);
    hub.shutdown().await.unwrap();
    server.await.unwrap();
}
#[tokio::test]
async fn metadata_expiry_invalidates_without_a_new_network_request() {
    let (cache, server) = metadata_cache(
        vec![(200, meta(false, 4))],
        CachePolicy {
            refresh_after: Duration::from_secs(10),
            max_age: Duration::from_secs(30),
            failure_retry: Duration::from_millis(10),
        },
    )
    .await;
    cache.refresh(false).await.unwrap();
    let (l, url) = listener().await;
    let mut hub = MarketHub::new();
    hub.bind_metadata(cache, SymbolMappings::default()).unwrap();
    let mut eth = hub
        .subscribe_at(key(Venue::Hyperliquid, "ETH"), url, timing())
        .await
        .unwrap();
    let mut s = accept(&l).await;
    subscriptions(&mut s, 2).await;
    hl(&mut s, "ETH", "100").await;
    until(&mut eth, |v| v.connected).await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(31)).await;
    until(&mut eth, |v| !v.connected).await;
    assert!(timeout(Duration::from_millis(100), l.accept())
        .await
        .is_err());
    hub.shutdown().await.unwrap();
    server.await.unwrap();
}
#[tokio::test]
async fn consumer_handoff_leaves_other_old_symbol_consumer_alive() {
    let (l, url) = listener().await;
    let mut hub = MarketHub::new();
    let mut first = hub
        .subscribe_at(key(Venue::Aster, "ETHUSDT"), url.clone(), timing())
        .await
        .unwrap();
    let mut other = first.clone();
    let mut s = accept(&l).await;
    subscriptions(&mut s, 1).await;
    aster(&mut s, "ETHUSDT", 1, "100").await;
    until(&mut first, |v| v.connected).await;
    hub.handoff(&mut first, key(Venue::Aster, "BTCUSDT"))
        .await
        .unwrap();
    assert_eq!(first.key().symbol(), "BTCUSDT");
    assert!(!first.snapshot().connected);
    let cmds = subscriptions(&mut s, 1).await;
    assert_eq!(cmds[0]["method"], "SUBSCRIBE");
    aster(&mut s, "BTCUSDT", 1, "500").await;
    aster(&mut s, "ETHUSDT", 2, "101").await;
    until(&mut first, |v| v.connected).await;
    until(&mut other, |v| v.accepted_depth >= 2).await;
    assert_eq!(hub.stats(Venue::Aster).connections_started, 1);
    hub.shutdown().await.unwrap();
}
#[tokio::test]
async fn remove_during_pending_subscribe_ack_unsubscribes_without_publishing() {
    let (l, url) = listener().await;
    let mut hub = MarketHub::new();
    let _eth = hub
        .subscribe_at(key(Venue::Aster, "ETHUSDT"), url.clone(), timing())
        .await
        .unwrap();
    let mut s = accept(&l).await;
    subscriptions(&mut s, 1).await;
    let btc = hub
        .subscribe_at(key(Venue::Aster, "BTCUSDT"), url, timing())
        .await
        .unwrap();
    let pending = command(&mut s).await;
    drop(btc);
    tokio::time::sleep(Duration::from_millis(20)).await;
    ack(&mut s, &pending).await;
    let remove = command(&mut s).await;
    assert_eq!(remove["method"], "UNSUBSCRIBE");
    ack(&mut s, &remove).await;
    assert_eq!(hub.stats(Venue::Aster).connections_started, 1);
    hub.shutdown().await.unwrap();
}
#[tokio::test]
async fn native_unicode_control_and_ping_pong_are_supported() {
    let (l, url) = listener().await;
    let mut hub = MarketHub::new();
    let mut sub = hub
        .subscribe_at(key(Venue::Aster, "哈基米USDT"), url, timing())
        .await
        .unwrap();
    let mut s = accept(&l).await;
    let c = command(&mut s).await;
    assert_eq!(c["params"][0], "哈基米usdt@depth10@100ms");
    ack(&mut s, &c).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    s.send(Message::Ping(vec![1, 2, 3].into())).await.unwrap();
    let pong = timeout(Duration::from_secs(1), s.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(pong, Message::Pong(vec![1, 2, 3].into()));
    aster(&mut s, "哈基米USDT", 1, "1").await;
    until(&mut sub, |v| v.connected).await;
    assert_eq!(hub.stats(Venue::Aster).heartbeat_frames_sent, 1);
    hub.shutdown().await.unwrap();
}
#[tokio::test]
async fn capacity_is_bounded_and_released_when_consumers_drop() {
    let (l, url) = listener().await;
    let mut hub = MarketHub::new();
    let mut all = Vec::new();
    for i in 0..32 {
        all.push(
            hub.subscribe_at(
                key(Venue::Aster, &format!("T{i}USDT")),
                url.clone(),
                timing(),
            )
            .await
            .unwrap(),
        );
    }
    assert!(hub
        .subscribe_at(key(Venue::Aster, "EXTRAUSDT"), url.clone(), timing())
        .await
        .is_err());
    all.pop();
    all.push(
        hub.subscribe_at(key(Venue::Aster, "EXTRAUSDT"), url, timing())
            .await
            .unwrap(),
    );
    let _pending = l.accept().await.unwrap();
    hub.shutdown().await.unwrap();
    assert!(all.iter().all(|s| !s.snapshot().connected));
}
#[tokio::test(start_paused = true)]
async fn reconnect_budget_cannot_be_reset_by_a_new_socket() {
    let mut p = Pacer::new(5, Duration::from_secs(60), Duration::ZERO);
    for _ in 0..5 {
        assert!(p.take(false));
    }
    assert!(!p.take(false));
    tokio::time::advance(Duration::from_secs(59)).await;
    assert!(!p.take(false));
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(p.take(false));
}

#[tokio::test]
async fn dropping_hub_clears_views_even_with_live_subscribers() {
    let (l, url) = listener().await;
    let mut hub = MarketHub::new();
    let mut sub = hub
        .subscribe_at(key(Venue::Aster, "ETHUSDT"), url, timing())
        .await
        .unwrap();
    let mut s = accept(&l).await;
    subscriptions(&mut s, 1).await;
    aster(&mut s, "ETHUSDT", 1, "100").await;
    until(&mut sub, |v| v.connected).await;
    drop(hub);
    until(&mut sub, |v| !v.connected).await;
    assert!(sub.snapshot().selected(wall_ms()).is_none());
}

#[tokio::test]
async fn last_drop_cancels_stalled_handshake_without_reconnecting() {
    let (l, url) = listener().await;
    let mut hub = MarketHub::new();
    let sub = hub
        .subscribe_at(key(Venue::Aster, "ETHUSDT"), url, timing())
        .await
        .unwrap();
    let (mut tcp, _) = l.accept().await.unwrap();
    drop(sub);
    timeout(Duration::from_millis(300), async {
        let mut b = [0u8; 1024];
        loop {
            match tcp.read(&mut b).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
    })
    .await
    .unwrap();
    assert!(timeout(Duration::from_millis(100), l.accept())
        .await
        .is_err());
    hub.shutdown().await.unwrap();
}

#[tokio::test]
async fn mismatched_hl_ack_and_duplicate_ack_are_not_quotes() {
    for duplicate in [false, true] {
        let (l, url) = listener().await;
        let mut hub = MarketHub::new();
        let mut sub = hub
            .subscribe_at(key(Venue::Hyperliquid, "ETH"), url, timing())
            .await
            .unwrap();
        let mut s = accept(&l).await;
        let c = command(&mut s).await;
        if duplicate {
            ack(&mut s, &c).await;
            let _second = command(&mut s).await;
            ack(&mut s, &c).await;
        } else {
            let mut wrong = c;
            wrong["subscription"]["coin"] = json!("BTC");
            ack(&mut s, &wrong).await;
        }
        until(&mut sub, |v| {
            v.reason.as_deref() == Some("mismatched subscription ACK")
        })
        .await;
        assert!(!sub.snapshot().connected);
        hub.shutdown().await.unwrap();
    }
}
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};
type Socket = WebSocketStream<TcpStream>;
fn timing() -> Timing {
    Timing {
        connect: Duration::from_secs(1),
        send: Duration::from_secs(1),
        idle: Duration::from_secs(3),
        backoff: Duration::from_millis(50),
        max_backoff: Duration::from_millis(100),
        heartbeat: Duration::from_secs(2),
        ack: Duration::from_secs(2),
        maintenance: Duration::from_millis(10),
        control_spacing: Some(Duration::from_millis(5)),
        connection_window: Duration::from_millis(500),
    }
}
fn key(venue: Venue, symbol: &str) -> MarketKey {
    MarketKey::new(venue, symbol).unwrap()
}
async fn listener() -> (TcpListener, String) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", l.local_addr().unwrap());
    (l, url)
}
async fn accept(l: &TcpListener) -> Socket {
    timeout(Duration::from_secs(3), async {
        let tcp = l.accept().await.unwrap().0;
        tcp.set_nodelay(true).unwrap();
        accept_async(tcp).await.unwrap()
    })
    .await
    .unwrap()
}
async fn until(s: &mut Subscription, p: impl Fn(&View) -> bool) -> Arc<View> {
    timeout(Duration::from_secs(3), async {
        loop {
            let v = s.snapshot();
            if p(&v) {
                return v;
            }
            s.changed().await.unwrap();
        }
    })
    .await
    .unwrap()
}
async fn command(s: &mut Socket) -> Value {
    let raw = timeout(Duration::from_secs(3), s.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_text()
        .unwrap();
    serde_json::from_str(&raw).unwrap()
}
async fn ack(s: &mut Socket, v: &Value) {
    let ack = if v.get("id").is_some() {
        json!({"id":v["id"],"result":null})
    } else {
        json!({"channel":"subscriptionResponse","data":v})
    };
    s.send(Message::Text(ack.to_string().into())).await.unwrap();
}
async fn subscriptions(s: &mut Socket, n: usize) -> Vec<Value> {
    let mut all = Vec::new();
    for _ in 0..n {
        let c = command(s).await;
        ack(s, &c).await;
        all.push(c);
    }
    tokio::time::sleep(Duration::from_millis(10)).await;
    all
}
async fn aster(s: &mut Socket, symbol: &str, seq: u64, price: &str) {
    s.send(Message::Text(json!({"e":"depthUpdate","s":symbol,"T":wall_ms(),"u":seq,"b":[[price,"2"]],"a":[["99999","2"]]}).to_string().into())).await.unwrap();
}
async fn hl(s: &mut Socket, symbol: &str, price: &str) {
    s.send(Message::Text(json!({"channel":"l2Book","data":{"coin":symbol,"time":wall_ms(),"levels":[[{"px":price,"sz":"2"}],[{"px":"99999","sz":"2"}]]}}).to_string().into())).await.unwrap();
}

#[tokio::test]
async fn two_aster_markets_and_duplicate_consumers_share_one_socket() {
    let (l, url) = listener().await;
    let mut hub = MarketHub::new();
    let mut eth = hub
        .subscribe_at(key(Venue::Aster, "ETHUSDT"), url.clone(), timing())
        .await
        .unwrap();
    let mut btc = hub
        .subscribe_at(key(Venue::Aster, "BTCUSDT"), url.clone(), timing())
        .await
        .unwrap();
    let mut duplicate = hub
        .subscribe_at(key(Venue::Aster, "ethusdt"), url, timing())
        .await
        .unwrap();
    let mut s = accept(&l).await;
    let commands = subscriptions(&mut s, 2).await;
    assert!(commands
        .iter()
        .all(|c| c["method"] == "SUBSCRIBE" && c["params"].as_array().unwrap().len() == 2));
    aster(&mut s, "ETHUSDT", 1, "100").await;
    aster(&mut s, "BTCUSDT", 1, "500").await;
    let e = until(&mut eth, |v| v.connected).await;
    let d = until(&mut duplicate, |v| v.connected).await;
    assert!(Arc::ptr_eq(&e, &d));
    assert_eq!(
        until(&mut btc, |v| v.connected)
            .await
            .selected(wall_ms())
            .unwrap()
            .symbol,
        "BTCUSDT"
    );
    assert!(timeout(Duration::from_millis(30), l.accept())
        .await
        .is_err());
    assert_eq!(hub.stats(Venue::Aster).connections_started, 1);
    hub.shutdown().await.unwrap();
    assert!(eth.snapshot().selected(wall_ms()).is_none());
    assert!(btc.snapshot().selected(wall_ms()).is_none());
}
#[tokio::test]
async fn hyperliquid_routes_native_case_and_both_channels_only_after_ack() {
    let (l, url) = listener().await;
    let mut hub = MarketHub::new();
    let mut eth = hub
        .subscribe_at(key(Venue::Hyperliquid, "ETH"), url.clone(), timing())
        .await
        .unwrap();
    let mut pepe = hub
        .subscribe_at(key(Venue::Hyperliquid, "kPEPE"), url, timing())
        .await
        .unwrap();
    let mut s = accept(&l).await;
    let c = command(&mut s).await;
    hl(&mut s, "ETH", "100").await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(!eth.snapshot().connected);
    ack(&mut s, &c).await;
    let mut cmds = vec![c];
    cmds.extend(subscriptions(&mut s, 3).await);
    assert_eq!(
        cmds.iter()
            .filter(|c| c["subscription"]["type"] == "l2Book" && c["subscription"]["fast"] == true)
            .count(),
        2
    );
    assert!(!eth.snapshot().connected, "ACK is not market data");
    hl(&mut s, "ETH", "100").await;
    hl(&mut s, "kPEPE", "1").await;
    until(&mut eth, |v| v.connected).await;
    until(&mut pepe, |v| v.connected).await;
    hub.shutdown().await.unwrap();
}
#[tokio::test]
async fn adding_and_removing_market_preserves_other_market_on_socket() {
    let (l, url) = listener().await;
    let mut hub = MarketHub::new();
    let mut eth = hub
        .subscribe_at(key(Venue::Aster, "ETHUSDT"), url.clone(), timing())
        .await
        .unwrap();
    let mut s = accept(&l).await;
    subscriptions(&mut s, 1).await;
    aster(&mut s, "ETHUSDT", 1, "100").await;
    let e = until(&mut eth, |v| v.connected).await;
    let btc = hub
        .subscribe_at(key(Venue::Aster, "BTCUSDT"), url, timing())
        .await
        .unwrap();
    subscriptions(&mut s, 1).await;
    drop(btc);
    let remove = command(&mut s).await;
    assert_eq!(remove["method"], "UNSUBSCRIBE");
    assert_eq!(remove["params"][0], "btcusdt@depth10@100ms");
    ack(&mut s, &remove).await;
    aster(&mut s, "BTCUSDT", 99, "500").await;
    aster(&mut s, "ETHUSDT", 2, "101").await;
    let later = until(&mut eth, |v| v.revision > e.revision).await;
    assert_eq!(later.generation, e.generation);
    assert_eq!(hub.stats(Venue::Aster).connections_started, 1);
    hub.shutdown().await.unwrap();
}
#[tokio::test]
async fn same_symbol_drop_readd_fences_old_socket() {
    let (l, url) = listener().await;
    let mut hub = MarketHub::new();
    let mut eth = hub
        .subscribe_at(key(Venue::Aster, "ETHUSDT"), url.clone(), timing())
        .await
        .unwrap();
    let _btc = hub
        .subscribe_at(key(Venue::Aster, "BTCUSDT"), url.clone(), timing())
        .await
        .unwrap();
    let mut s = accept(&l).await;
    subscriptions(&mut s, 2).await;
    aster(&mut s, "ETHUSDT", 99, "100").await;
    until(&mut eth, |v| v.connected).await;
    drop(eth);
    let mut next = hub
        .subscribe_at(key(Venue::Aster, "ETHUSDT"), url, timing())
        .await
        .unwrap();
    assert!(!next.snapshot().connected);
    let _=s.send(Message::Text(json!({"e":"depthUpdate","s":"ETHUSDT","T":wall_ms(),"u":100,"b":[["999","2"]],"a":[["1000","2"]]}).to_string().into())).await;
    let mut s2 = accept(&l).await;
    subscriptions(&mut s2, 2).await;
    assert!(!next.snapshot().connected);
    aster(&mut s2, "ETHUSDT", 1, "101").await;
    assert_eq!(
        until(&mut next, |v| v.connected)
            .await
            .selected(wall_ms())
            .unwrap()
            .quote
            .bid()
            .to_string(),
        "101"
    );
    hub.shutdown().await.unwrap();
}
#[tokio::test]
async fn closing_socket_invalidates_all_and_reconnect_resets_sequence() {
    let (l, url) = listener().await;
    let mut hub = MarketHub::new();
    let mut eth = hub
        .subscribe_at(key(Venue::Aster, "ETHUSDT"), url.clone(), timing())
        .await
        .unwrap();
    let mut btc = hub
        .subscribe_at(key(Venue::Aster, "BTCUSDT"), url, timing())
        .await
        .unwrap();
    let mut s = accept(&l).await;
    subscriptions(&mut s, 2).await;
    aster(&mut s, "ETHUSDT", 100, "100").await;
    aster(&mut s, "BTCUSDT", 100, "500").await;
    let e = until(&mut eth, |v| v.connected).await;
    until(&mut btc, |v| v.connected).await;
    s.close(None).await.unwrap();
    until(&mut eth, |v| !v.connected).await;
    assert!(!btc.snapshot().connected);
    let mut s2 = accept(&l).await;
    subscriptions(&mut s2, 2).await;
    aster(&mut s2, "ETHUSDT", 1, "101").await;
    assert!(until(&mut eth, |v| v.connected).await.generation > e.generation);
    hub.shutdown().await.unwrap();
}
#[tokio::test]
async fn active_btc_and_heartbeats_cannot_hide_frozen_eth() {
    let (l, url) = listener().await;
    let mut hub = MarketHub::new();
    let mut t = timing();
    t.idle = Duration::from_secs(2);
    let mut eth = hub
        .subscribe_at(key(Venue::Aster, "ETHUSDT"), url.clone(), t)
        .await
        .unwrap();
    let _btc = hub
        .subscribe_at(key(Venue::Aster, "BTCUSDT"), url, t)
        .await
        .unwrap();
    let mut s = accept(&l).await;
    subscriptions(&mut s, 2).await;
    aster(&mut s, "ETHUSDT", 1, "100").await;
    until(&mut eth, |v| v.connected).await;
    let mut tick = tokio::time::interval(Duration::from_millis(15));
    let mut seq = 0;
    timeout(Duration::from_secs(3),async{loop{tokio::select!{_=tick.tick()=>{seq+=1;let _=s.send(Message::Text(json!({"e":"depthUpdate","s":"BTCUSDT","T":wall_ms(),"u":seq,"b":[["500","2"]],"a":[["501","2"]]}).to_string().into())).await;let _=s.send(Message::Pong(Vec::new().into())).await;},v=eth.changed()=>{if !v.unwrap().connected{break;}}}}}).await.unwrap();
    assert_eq!(
        hub.stats(Venue::Aster).last_error.as_deref(),
        Some("individual market data timeout")
    );
    hub.shutdown().await.unwrap();
}
#[tokio::test]
async fn malformed_frame_or_wrong_ack_fails_closed() {
    for raw in [
        "{broken",
        "{\"id\":999,\"result\":null}",
        "{\"code\":2,\"msg\":\"invalid\"}",
    ] {
        let (l, url) = listener().await;
        let mut hub = MarketHub::new();
        let mut sub = hub
            .subscribe_at(key(Venue::Aster, "ETHUSDT"), url, timing())
            .await
            .unwrap();
        let mut s = accept(&l).await;
        let _ = command(&mut s).await;
        let before = sub.snapshot().revision;
        s.send(Message::Text(raw.into())).await.unwrap();
        until(&mut sub, |v| {
            !v.connected && v.revision > before && v.reason.as_deref() != Some("connecting")
        })
        .await;
        assert_eq!(hub.stats(Venue::Aster).faults, 1);
        hub.shutdown().await.unwrap();
    }
}
#[tokio::test]
async fn missing_ack_times_out_even_when_quotes_arrive() {
    let (l, url) = listener().await;
    let mut hub = MarketHub::new();
    let mut t = timing();
    t.ack = Duration::from_millis(80);
    let mut sub = hub
        .subscribe_at(key(Venue::Aster, "ETHUSDT"), url, t)
        .await
        .unwrap();
    let mut s = accept(&l).await;
    let _ = command(&mut s).await;
    aster(&mut s, "ETHUSDT", 1, "100").await;
    until(&mut sub, |v| {
        v.reason.as_deref() == Some("subscription ACK timeout")
    })
    .await;
    assert!(!sub.snapshot().connected);
    hub.shutdown().await.unwrap();
}
#[tokio::test]
async fn last_consumer_and_stalled_handshake_can_stop_promptly() {
    let (l, url) = listener().await;
    let mut hub = MarketHub::new();
    let sub = hub
        .subscribe_at(key(Venue::Aster, "ETHUSDT"), url, timing())
        .await
        .unwrap();
    let _pending = l.accept().await.unwrap();
    timeout(Duration::from_millis(300), hub.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert!(!sub.snapshot().connected);
    let (l, url) = listener().await;
    let mut hub = MarketHub::new();
    let sub = hub
        .subscribe_at(key(Venue::Aster, "ETHUSDT"), url, timing())
        .await
        .unwrap();
    let mut s = accept(&l).await;
    subscriptions(&mut s, 1).await;
    drop(sub);
    let closed = timeout(Duration::from_millis(300), s.next()).await.unwrap();
    assert!(matches!(
        closed,
        None | Some(Err(_)) | Some(Ok(Message::Close(_)))
    ));
    hub.shutdown().await.unwrap();
}
#[tokio::test(start_paused = true)]
async fn pacing_reserves_pongs_and_preserves_rolling_window() {
    let mut p = Pacer::new(8, Duration::from_secs(1), Duration::from_millis(100));
    assert!(p.take(true));
    assert!(!p.take(true));
    for _ in 0..5 {
        tokio::time::advance(Duration::from_millis(100)).await;
        assert!(p.take(true));
    }
    assert!(!p.take(true));
    assert!(p.take(false));
    assert!(p.take(false));
    assert!(!p.take(false));
    let ready = p.ready(true);
    assert!(ready > Instant::now());
    tokio::time::advance(ready - Instant::now()).await;
    assert!(p.take(true));
}
#[tokio::test]
async fn limited_control_sends_do_not_block_existing_market_reads() {
    let (l, url) = listener().await;
    let mut hub = MarketHub::new();
    let mut t = timing();
    t.control_spacing = Some(Duration::from_millis(150));
    let mut eth = hub
        .subscribe_at(key(Venue::Aster, "ETHUSDT"), url.clone(), t)
        .await
        .unwrap();
    let mut s = accept(&l).await;
    subscriptions(&mut s, 1).await;
    aster(&mut s, "ETHUSDT", 1, "100").await;
    let initial = until(&mut eth, |v| v.connected).await;
    let _btc = hub
        .subscribe_at(key(Venue::Aster, "BTCUSDT"), url, t)
        .await
        .unwrap();
    aster(&mut s, "ETHUSDT", 2, "101").await;
    timeout(
        Duration::from_millis(100),
        until(&mut eth, |v| v.revision > initial.revision),
    )
    .await
    .unwrap();
    let c = command(&mut s).await;
    assert_eq!(c["params"][0], "btcusdt@depth10@100ms");
    ack(&mut s, &c).await;
    hub.shutdown().await.unwrap();
}
