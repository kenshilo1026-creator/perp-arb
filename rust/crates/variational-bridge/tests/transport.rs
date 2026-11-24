use futures_util::{SinkExt, StreamExt};
use perp_arb_variational_bridge::{
    wire::{Channel, Event, Observation},
    Bridge, Config, ReadRequest,
};
use serde_json::{json, Value};
use std::{net::SocketAddr, time::Duration};
use tokio::{net::TcpStream, sync::mpsc, time::timeout};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message},
    MaybeTlsStream, WebSocketStream,
};

const ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[tokio::test]
async fn mid_preview_uses_only_correlated_read_request_without_trading_opt_in() {
    let (bridge, mut rx) = Bridge::bind(config(32)).await.unwrap();
    let mut socket = registered(&bridge, &mut rx).await;
    let responder = async {
        let command = read(&mut socket).await;
        assert_eq!(command["type"], "PREVIEW_LIMIT_ORDER_PRICE");
        assert_eq!(command["symbol"], "ETH");
        assert!(command.get("amount").is_none());
        send(
            &mut socket,
            json!({"type":"PRICE_PREVIEW_RESULT", "requestId":"wrong-id", "ok":true, "price":"1"}),
        )
        .await;
        send(&mut socket, json!({"type":"PRICE_PREVIEW_RESULT", "requestId":command["requestId"], "ok":true, "price":"3001.25"})).await;
    };
    let (result, ()) = tokio::join!(
        perp_arb_variational_bridge::mid_preview::request(&bridge, "ETH", Duration::from_secs(1)),
        responder
    );
    let result = result.unwrap();
    assert_eq!(result["price"], "3001.25");
    assert_eq!(result["source"], "variational_page_mid_button");
    assert_eq!(result["can_submit_orders"], false);
    bridge.shutdown().await;
}

#[tokio::test]
async fn mid_preview_missing_extension_times_out_without_order() {
    let (bridge, _rx) = Bridge::bind(config(32)).await.unwrap();
    let result =
        perp_arb_variational_bridge::mid_preview::request(&bridge, "ETH", Duration::from_millis(5))
            .await;
    assert!(result.unwrap_err().contains("Mid preview timeout"));
    bridge.shutdown().await;
}

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
fn config(capacity: usize) -> Config {
    Config {
        extension_id: ID.into(),
        private_port: 0,
        reference_port: 0,
        command_port: 0,
        queue_capacity: capacity,
    }
}
async fn connect(address: SocketAddr) -> Socket {
    let mut request = format!("ws://{address}").into_client_request().unwrap();
    request.headers_mut().insert(
        "origin",
        format!("chrome-extension://{ID}").parse().unwrap(),
    );
    timeout(Duration::from_secs(3), connect_async(request))
        .await
        .unwrap()
        .unwrap()
        .0
}
async fn send(socket: &mut Socket, value: Value) {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
async fn read(socket: &mut Socket) -> Value {
    let message = timeout(Duration::from_secs(3), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_str(message.to_text().unwrap()).unwrap()
}
async fn event(rx: &mut mpsc::Receiver<Event>) -> Event {
    timeout(Duration::from_secs(3), rx.recv())
        .await
        .unwrap()
        .unwrap()
}
async fn registered(bridge: &Bridge, rx: &mut mpsc::Receiver<Event>) -> Socket {
    let mut socket = connect(bridge.command_address).await;
    send(&mut socket, json!({"type":"REGISTER","role":"extension"})).await;
    assert_eq!(read(&mut socket).await["type"], "REGISTER_ACK");
    let ready = event(rx).await;
    assert_eq!(ready.channel, Channel::Command);
    assert_eq!(ready.observation, Observation::Connected);
    socket
}

async fn registered_native(
    bridge: &Bridge,
    rx: &mut mpsc::Receiver<Event>,
    enabled: bool,
) -> Socket {
    let mut socket = connect(bridge.command_address).await;
    send(&mut socket,json!({"type":"REGISTER","role":"extension","nativeProtocol":"variational-native-v1","nativeTradingEnabled":enabled})).await;
    assert_eq!(read(&mut socket).await["type"], "REGISTER_ACK");
    event(rx).await;
    socket
}

fn native_ledger() -> (
    std::path::PathBuf,
    perp_arb_variational_bridge::orders::Ledger,
) {
    use perp_arb_variational_bridge::orders::{Command, Ledger};
    let mut bytes = [0; 16];
    getrandom::fill(&mut bytes).unwrap();
    let path = std::env::temp_dir().join(format!(
        "native-transport-{:x}.wal",
        u128::from_le_bytes(bytes)
    ));
    let ledger = Ledger::create(
        &path,
        Command {
            request_id: "native-submit-1".into(),
            account: format!("0x{}", "a".repeat(40)),
            symbol: "ETH".into(),
            side: "SELL".into(),
            quantity: "2".into(),
            limit_price: "100".into(),
            reduce_only: true,
            max_notional: "200".into(),
        },
    )
    .unwrap();
    (path, ledger)
}
fn native_instrument() -> perp_arb_variational_bridge::native::Instrument {
    perp_arb_variational_bridge::native::Instrument {
        underlying: "ETH".into(),
        instrument_type: "perpetual_future".into(),
        settlement_asset: "USDC".into(),
        funding_interval_s: 3600,
    }
}
#[tokio::test]
async fn native_submit_binds_only_correlated_real_rfq_ack_without_counting_a_fill() {
    let (bridge, mut rx) = Bridge::bind(config(32)).await.unwrap();
    let mut socket = registered_native(&bridge, &mut rx, true).await;
    let (path, mut ledger) = native_ledger();
    let venue = async {
        let request = read(&mut socket).await;
        assert_eq!(request["action"], "submit");
        assert_eq!(request["order"]["is_reduce_only"], true);
        assert_eq!(request["order"]["slippage_limit"], "0");
        let now = perp_arb_variational_bridge::wire::now_ms().unwrap();
        send(&mut socket,json!({"type":"NATIVE_ORDER_RESULT","requestId":request["requestId"],"ok":true,
            "receipt":{"protocol":"variational-native-v1","started_ms":now,"finished_ms":now,"account":request["account"],
            "session_continuity":true,"ok":true,"dispatched":true,"rfq_id":"server-rfq-1","source":"https://omni.variational.io/api/orders/new/limit"}})).await;
    };
    let (result, ()) = tokio::join!(
        bridge.submit_native(ledger.dispatch().unwrap(), native_instrument()),
        venue
    );
    result.unwrap().record(&mut ledger).unwrap();
    assert_eq!(ledger.view().order_id.as_deref(), Some("server-rfq-1"));
    assert_eq!(ledger.view().cumulative, "0");
    assert!(!ledger.view().terminal_confirmed);
    assert!(ledger.dispatch().is_err());
    drop(ledger);
    std::fs::remove_file(path).unwrap();
    bridge.shutdown().await;
}
#[tokio::test]
async fn native_unknown_submit_and_disabled_extension_never_replay() {
    for enabled in [false, true] {
        let (bridge, mut rx) = Bridge::bind(config(32)).await.unwrap();
        let mut socket = registered_native(&bridge, &mut rx, enabled).await;
        let (path, mut ledger) = native_ledger();
        let venue = async {
            if enabled {
                assert_eq!(read(&mut socket).await["action"], "submit");
                socket.close(None).await.unwrap();
            }
        };
        let (result, ()) = tokio::join!(
            bridge.submit_native(ledger.dispatch().unwrap(), native_instrument()),
            venue
        );
        assert!(result.is_err());
        assert!(ledger.view().order_id.is_none());
        assert!(ledger.dispatch().is_err());
        drop(ledger);
        let mut reopened = perp_arb_variational_bridge::orders::Ledger::open(&path).unwrap();
        assert!(reopened.dispatch().is_err());
        drop(reopened);
        std::fs::remove_file(path).unwrap();
        bridge.shutdown().await;
    }
}
fn portfolio() -> Value {
    let now = perp_arb_variational_bridge::wire::now_ms().unwrap();
    json!({"kind":"ws_frame","direction":"received","opcode":1,
        "timestamp":chrono::DateTime::from_timestamp_millis(now).unwrap().to_rfc3339(),
        "url":"wss://omni-ws-server.prod.ap-northeast-1.variational.io/portfolio",
        "payloadData":"{\"positions\":[]}"})
}

#[tokio::test]
async fn handshake_rejects_untrusted_origin_and_listener_recovers() {
    let (bridge, mut rx) = Bridge::bind(config(32)).await.unwrap();
    for origin in [
        None,
        Some("https://omni.variational.io"),
        Some("chrome-extension://bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
    ] {
        let mut request = format!("ws://{}", bridge.private_address)
            .into_client_request()
            .unwrap();
        if let Some(origin) = origin {
            request
                .headers_mut()
                .insert("origin", origin.parse().unwrap());
        }
        let err = timeout(Duration::from_secs(3), connect_async(request))
            .await
            .unwrap()
            .unwrap_err();
        assert!(err.to_string().contains("403"));
    }
    assert!(!bridge.health().borrow().private_connected);
    let _socket = connect(bridge.private_address).await;
    assert_eq!(event(&mut rx).await.observation, Observation::Connected);
    bridge.shutdown().await;
}

#[tokio::test]
async fn private_events_and_reconnect_have_new_generation() {
    let (bridge, mut rx) = Bridge::bind(config(32)).await.unwrap();
    let mut socket = connect(bridge.private_address).await;
    let first = event(&mut rx).await;
    send(&mut socket, portfolio()).await;
    let observed = event(&mut rx).await;
    assert_eq!(
        observed.observation,
        Observation::Portfolio {
            positions: vec![],
            wallet_balance: None
        }
    );
    assert_eq!(observed.generation, first.generation);
    assert!(observed.captured_ms.is_some());
    socket.close(None).await.unwrap();
    assert_eq!(event(&mut rx).await.observation, Observation::Disconnected);
    let _next = connect(bridge.private_address).await;
    assert!(event(&mut rx).await.generation > first.generation);
    bridge.shutdown().await;
}

#[tokio::test]
async fn malformed_capture_emits_gap_instead_of_silent_drop() {
    let (bridge, mut rx) = Bridge::bind(config(32)).await.unwrap();
    let mut socket = connect(bridge.private_address).await;
    event(&mut rx).await;
    let mut old = portfolio();
    old["timestamp"] = json!("2000-01-01T00:00:00Z");
    send(&mut socket, old).await;
    assert!(matches!(
        event(&mut rx).await.observation,
        Observation::Gap { .. }
    ));
    send(&mut socket, portfolio()).await;
    assert!(matches!(
        event(&mut rx).await.observation,
        Observation::Portfolio { .. }
    ));
    bridge.shutdown().await;
}

#[tokio::test]
async fn read_requires_registration_and_ignores_unrelated_ack() {
    let (bridge, mut rx) = Bridge::bind(config(32)).await.unwrap();
    assert!(bridge
        .request(ReadRequest::PreviewPrice {
            symbol: "ETH".into()
        })
        .await
        .is_err());
    let mut socket = registered(&bridge, &mut rx).await;
    let extension = async {
        let request = read(&mut socket).await;
        assert_eq!(request["type"], "PREVIEW_LIMIT_ORDER_PRICE");
        send(
            &mut socket,
            json!({"type":"ORDER_RESULT","requestId":"old","ok":true,"filled":true}),
        )
        .await;
        send(
            &mut socket,
            json!({"type":"PRICE_PREVIEW_RESULT","requestId":request["requestId"],
            "ok":true,"price":"2345.67","details":{"token":"must-not-return"}}),
        )
        .await;
    };
    let (result, ()) = tokio::join!(
        bridge.request(ReadRequest::PreviewPrice {
            symbol: "ETH-PERP".into()
        }),
        extension
    );
    assert_eq!(
        result.unwrap(),
        json!({"price":"2345.67","indicative_only":true})
    );
    bridge.shutdown().await;
}

#[tokio::test]
async fn check_order_is_readonly_and_absence_is_not_terminal_proof() {
    let (bridge, mut rx) = Bridge::bind(config(32)).await.unwrap();
    let mut socket = registered(&bridge, &mut rx).await;
    let extension = async {
        let request = read(&mut socket).await;
        assert_eq!(request["type"], "CANCEL_ORDER");
        assert_eq!(request["checkOnly"], true);
        assert_eq!(request["orderId"], "order-1");
        send(
            &mut socket,
            json!({"type":"CANCEL_RESULT","requestId":request["requestId"],"ok":true,
            "details":{"exists":false}}),
        )
        .await;
    };
    let (result, ()) = tokio::join!(
        bridge.request(ReadRequest::CheckOpenOrder {
            symbol: "ETH".into(),
            order_id: Some("order-1".into())
        }),
        extension
    );
    assert_eq!(
        result.unwrap(),
        json!({"exists":false,"terminal_proven":false,"order_identity_proven":false})
    );
    bridge.shutdown().await;
}

#[tokio::test]
async fn missing_presence_never_defaults_to_absent() {
    let (bridge, mut rx) = Bridge::bind(config(32)).await.unwrap();
    let mut socket = registered(&bridge, &mut rx).await;
    let extension = async {
        let request = read(&mut socket).await;
        send(
            &mut socket,
            json!({"type":"CANCEL_RESULT","requestId":request["requestId"],"ok":true}),
        )
        .await;
    };
    let (result, ()) = tokio::join!(
        bridge.request(ReadRequest::CheckOpenOrder {
            symbol: "ETH".into(),
            order_id: None
        }),
        extension
    );
    assert!(result.unwrap_err().contains("presence"));
    bridge.shutdown().await;
}

#[tokio::test]
async fn disconnect_fails_pending_read_and_does_not_replay_after_reconnect() {
    let (bridge, mut rx) = Bridge::bind(config(32)).await.unwrap();
    let mut socket = registered(&bridge, &mut rx).await;
    let extension = async {
        read(&mut socket).await;
        socket.close(None).await.unwrap();
    };
    let (result, ()) = tokio::join!(
        bridge.request(ReadRequest::PreviewPrice {
            symbol: "ETH".into()
        }),
        extension
    );
    assert!(result.is_err());
    assert!(matches!(
        event(&mut rx).await.observation,
        Observation::Gap { .. }
    ));
    assert_eq!(event(&mut rx).await.observation, Observation::Disconnected);
    let mut next = registered(&bridge, &mut rx).await;
    // An explicit new request is the first command on the replacement socket.
    let extension = async {
        let request = read(&mut next).await;
        assert_eq!(request["symbol"], "BTC");
        send(&mut next,json!({"type":"PRICE_PREVIEW_RESULT","requestId":request["requestId"],"ok":true,"price":"100"})).await;
    };
    let (result, ()) = tokio::join!(
        bridge.request(ReadRequest::PreviewPrice {
            symbol: "BTC".into()
        }),
        extension
    );
    assert!(result.is_ok());
    bridge.shutdown().await;
}

#[tokio::test]
async fn stalled_consumer_overflow_faults_all_channels() {
    let (bridge, _rx) = Bridge::bind(config(1)).await.unwrap();
    let mut health = bridge.health();
    let mut socket = connect(bridge.private_address).await;
    // Connected consumes the sole queue slot; the next event must fault.
    send(&mut socket, portfolio()).await;
    timeout(Duration::from_secs(3), async {
        loop {
            if health.borrow().fault.is_some() {
                break;
            }
            health.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert!(bridge
        .request(ReadRequest::PreviewPrice {
            symbol: "ETH".into()
        })
        .await
        .is_err());
    bridge.shutdown().await;
    assert!(!health.borrow().private_connected);
}

#[tokio::test]
async fn port_collision_releases_all_previously_acquired_ports() {
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let free = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let free_address = free.local_addr().unwrap();
    drop(free);
    let mut cfg = config(32);
    cfg.private_port = free_address.port();
    cfg.reference_port = occupied.local_addr().unwrap().port();
    assert!(Bridge::bind(cfg).await.is_err());
    let _released = tokio::net::TcpListener::bind(free_address).await.unwrap();
}

#[tokio::test]
async fn wrong_registration_never_enables_commands() {
    let (bridge, mut rx) = Bridge::bind(config(32)).await.unwrap();
    let mut socket = connect(bridge.command_address).await;
    send(&mut socket, json!({"type":"REGISTER","role":"client"})).await;
    assert!(matches!(
        event(&mut rx).await.observation,
        Observation::Gap { .. }
    ));
    assert!(!bridge.health().borrow().command_connected);
    bridge.shutdown().await;
}

#[tokio::test]
async fn pending_page_query_does_not_block_private_event_delivery() {
    let (bridge, mut rx) = Bridge::bind(config(32)).await.unwrap();
    let mut socket = registered(&bridge, &mut rx).await;
    let mut feed = connect(bridge.private_address).await;
    assert_eq!(event(&mut rx).await.observation, Observation::Connected);
    let extension = async {
        let request = read(&mut socket).await;
        // Withhold the page reply until a separate private frame is delivered.
        send(&mut feed, portfolio()).await;
        let observed = event(&mut rx).await;
        assert!(matches!(
            observed.observation,
            Observation::Portfolio { .. }
        ));
        assert!(observed.parsed_elapsed_us >= observed.received_elapsed_us);
        send(&mut socket,json!({"type":"PRICE_PREVIEW_RESULT","requestId":request["requestId"],"ok":true,"price":"100"})).await;
    };
    let (result, ()) = tokio::join!(
        bridge.request(ReadRequest::PreviewPrice {
            symbol: "ETH".into()
        }),
        extension
    );
    assert!(result.is_ok());
    bridge.shutdown().await;
}
