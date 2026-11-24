use super::*;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn payload() -> Value {
    json!({"listings": [{"ticker":"ETH-PERP", "contract_type":"perpetual",
        "quotes":{"updated_at":"2026-10-16T02:51:10.363561208Z",
        "base":{"bid":"99.99", "ask":"100.01"},
        "size_1k":{"bid":"99.9", "ask":"100.1"},
        "size_100k":{"bid":"99", "ask":"101"},
        "size_1m":{"bid":"98", "ask":"102"}}},
        {"ticker":"ETH-SWAP", "contract_type":"swap", "funding":null}]})
}
fn parse(value: Value) -> Result<Stats> {
    Stats::parse(&serde_json::to_vec(&value).unwrap(), 1792119070363)
}
fn d(s: &str) -> Decimal {
    decimal(s).unwrap()
}

#[test]
fn exact_decimal_and_source_nanoseconds_are_preserved_to_milliseconds() {
    let mut data = payload();
    data["listings"][0]["quotes"]["size_1k"]["bid"] = json!("99.90000000000000000000000001");
    let stats = parse(data).unwrap();
    let q = stats.perpetual_quote("eth", d("1000")).unwrap();
    assert_eq!(q.bid, d("99.90000000000000000000000001"));
    assert_eq!(q.source_ms, 1792119070363);
    assert_eq!(q.tier, "size_1k");
}

#[test]
fn tier_boundaries_and_large_notional_never_fall_back_to_base() {
    let stats = parse(payload()).unwrap();
    for (amount, expected) in [
        ("1", "size_1k"),
        ("1000", "size_1k"),
        ("1000.0001", "size_100k"),
        ("100000", "size_100k"),
        ("100000.01", "size_1m"),
        ("1000000", "size_1m"),
    ] {
        assert_eq!(
            stats.perpetual_quote("ETH-PERP", d(amount)).unwrap().tier,
            expected
        );
    }
    for amount in ["0", "-1", "1000000.01"] {
        assert!(stats.perpetual_quote("ETH", d(amount)).is_err());
    }
    let mut data = payload();
    data["listings"][0]["quotes"]
        .as_object_mut()
        .unwrap()
        .retain(|k, _| k == "base" || k == "updated_at");
    assert!(parse(data).unwrap().perpetual_quote("ETH", d("1")).is_err());
}

#[test]
fn missing_tier_can_use_larger_reference_but_invalid_tier_is_not_hidden() {
    let mut data = payload();
    data["listings"][0]["quotes"]
        .as_object_mut()
        .unwrap()
        .remove("size_1k");
    assert_eq!(
        parse(data.clone())
            .unwrap()
            .perpetual_quote("ETH", d("50"))
            .unwrap()
            .tier,
        "size_100k"
    );
    data["listings"][0]["quotes"]["size_1k"] = json!({"bid":"102", "ask":"100"});
    assert!(parse(data)
        .unwrap()
        .perpetual_quote("ETH", d("50"))
        .is_err());
}

#[test]
fn optional_10k_tier_matches_python_selection_when_available() {
    let mut data = payload();
    data["listings"][0]["quotes"]["size_10k"] = json!({"bid":"99.5", "ask":"100.5"});
    assert_eq!(
        parse(data)
            .unwrap()
            .perpetual_quote("ETH", d("10000"))
            .unwrap()
            .tier,
        "size_10k"
    );
}

#[test]
fn absent_quotes_swap_and_wrong_contract_are_never_perpetual_prices() {
    let stats = parse(payload()).unwrap();
    assert!(stats.perpetual_quote("ETH-SWAP", d("50")).is_err());
    assert!(stats.perpetual_quote("ETHUSDT", d("50")).is_err());
    let mut data = payload();
    data["listings"][0]
        .as_object_mut()
        .unwrap()
        .remove("quotes");
    assert!(parse(data)
        .unwrap()
        .perpetual_quote("ETH", d("50"))
        .is_err());
    let mut data = payload();
    data["listings"][0]["contract_type"] = json!("swap");
    assert!(parse(data).is_err());
}

#[test]
fn rejects_v1_duplicate_unknown_contract_and_bad_json() {
    for field in ["ticker", "contract_type"] {
        let mut data = payload();
        data["listings"][0].as_object_mut().unwrap().remove(field);
        assert!(parse(data).is_err());
    }
    let mut data = payload();
    let duplicate = data["listings"][0].clone();
    data["listings"].as_array_mut().unwrap().push(duplicate);
    assert!(parse(data).is_err());
    let mut data = payload();
    data["listings"][0]["contract_type"] = json!("future");
    assert!(parse(data).is_err());
    assert!(Stats::parse(b"[]", 1).is_err());
    assert!(Stats::parse(b"invalid", 1).is_err());
    assert!(Stats::parse(&vec![b' '; MAX_BODY + 1], 1).is_err());
}

#[test]
fn invalid_prices_are_rejected_without_binary_float_conversion() {
    for invalid in [
        json!(99.9),
        json!("NaN"),
        json!("0"),
        json!("-1"),
        json!("0.00000000000000000000000000001"),
        Value::Null,
    ] {
        let mut data = payload();
        data["listings"][0]["quotes"]["size_1k"]["bid"] = invalid;
        assert!(parse(data)
            .unwrap()
            .perpetual_quote("ETH", d("50"))
            .is_err());
    }
}

#[test]
fn timestamps_require_timezone_and_real_calendar_date() {
    for stamp in [
        "2026-10-16T02:51:10",
        "2026-02-30T01:00:00Z",
        "garbage",
        "1960-01-01T00:00:00Z",
    ] {
        let mut data = payload();
        data["listings"][0]["quotes"]["updated_at"] = json!(stamp);
        assert!(parse(data)
            .unwrap()
            .perpetual_quote("ETH", d("50"))
            .is_err());
    }
    let mut data = payload();
    data["listings"][0]["quotes"]["updated_at"] = json!("2026-10-16T10:51:10.363561208+08:00");
    assert_eq!(
        parse(data)
            .unwrap()
            .perpetual_quote("ETH", d("50"))
            .unwrap()
            .source_ms,
        1792119070363
    );
}

#[test]
fn recent_http_receipt_does_not_refresh_cached_exchange_quote() {
    let stats = parse(payload()).unwrap();
    let mut q = stats.perpetual_quote("ETH", d("50")).unwrap();
    let policy = FreshnessPolicy::default();
    assert_eq!(q.freshness(q.source_ms, policy), Freshness::Fresh);
    assert_eq!(q.freshness(q.source_ms + 600_000, policy), Freshness::Stale);
    assert_eq!(q.freshness(q.source_ms - 1000, policy), Freshness::Future);
    q.received_ms += 600;
    assert_eq!(
        q.freshness(q.source_ms + 600, policy),
        Freshness::TransportLag
    );
}

async fn server(
    responses: Vec<(u16, String, String)>,
) -> (String, tokio::task::JoinHandle<Vec<Instant>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "http://{}/metadata/v2/stats",
        listener.local_addr().unwrap()
    );
    let task = tokio::spawn(async move {
        let mut times = Vec::new();
        for (status, headers, body) in responses {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut bytes = [0; 1024];
                let n = socket.read(&mut bytes).await.unwrap();
                assert!(n > 0);
                request.extend_from_slice(&bytes[..n]);
                if request.windows(4).any(|x| x == b"\r\n\r\n") {
                    break;
                }
            }
            assert!(request.starts_with(b"GET /metadata/v2/stats HTTP/1.1"));
            assert!(!String::from_utf8_lossy(&request)
                .to_lowercase()
                .contains("authorization:"));
            times.push(Instant::now());
            socket.write_all(format!("HTTP/1.1 {status} Test\r\nConnection: close\r\nContent-Length: {}\r\n{headers}\r\n{body}", body.len()).as_bytes()).await.unwrap();
        }
        times
    });
    (url, task)
}

#[tokio::test]
async fn cloned_consumers_share_one_all_market_request_and_cache() {
    let (url, server) = server(vec![(200, String::new(), payload().to_string())]).await;
    let client = PublicClient::with_url(url).unwrap();
    let other = client.clone();
    let (a, b) = tokio::join!(client.snapshot(), other.snapshot());
    let (a, b) = (a.unwrap(), b.unwrap());
    assert!(Arc::ptr_eq(&a, &b));
    assert!(Arc::ptr_eq(&a, &client.snapshot().await.unwrap()));
    assert_eq!(server.await.unwrap().len(), 1);
}

#[tokio::test]
async fn expired_cache_refreshes_and_removed_quotes_do_not_survive() {
    let mut unavailable = payload();
    unavailable["listings"][0]
        .as_object_mut()
        .unwrap()
        .remove("quotes");
    let (url, server) = server(vec![
        (200, String::new(), payload().to_string()),
        (200, String::new(), unavailable.to_string()),
    ])
    .await;
    let client = PublicClient::with_url(url).unwrap();
    client.snapshot().await.unwrap();
    tokio::time::sleep(SPACING).await;
    assert!(client
        .snapshot()
        .await
        .unwrap()
        .perpetual_quote("ETH", d("50"))
        .is_err());
    let times = server.await.unwrap();
    assert!(times[1].duration_since(times[0]) >= SPACING);
}

#[tokio::test]
async fn rate_limit_keeps_longest_server_hint_and_invalidates_cache() {
    let (url, server) = server(vec![(
        429,
        "Retry-After: 60\r\n".into(),
        json!({"retry_after_seconds": "120.1"}).to_string(),
    )])
    .await;
    let client = PublicClient::with_url(url).unwrap();
    assert!(client.snapshot().await.unwrap_err().contains("429"));
    let state = client.state.lock().await;
    assert!(state.cached.is_none());
    assert!(state.next_request.duration_since(Instant::now()) > Duration::from_secs(120));
    drop(state);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), client.snapshot())
            .await
            .is_err()
    );
    assert_eq!(server.await.unwrap().len(), 1);
}

#[tokio::test]
async fn cooldown_survives_cancellation_and_huge_values_block_client() {
    let client = PublicClient::new().unwrap();
    let mut state = client.state.lock().await;
    assert!(defer(&mut state, Duration::from_secs(u64::MAX)).is_err());
    assert!(state.blocked);
    drop(state);
    assert!(client.snapshot().await.unwrap_err().contains("blocked"));
}

#[tokio::test]
async fn failed_refresh_does_not_return_old_cache() {
    for (status, body) in [(503, "unavailable"), (200, "invalid JSON")] {
        let (url, server) = server(vec![(status, String::new(), body.to_owned())]).await;
        let client = PublicClient::with_url(url).unwrap();
        client.state.lock().await.cached = Some((
            Instant::now() - Duration::from_secs(3),
            Arc::new(parse(payload()).unwrap()),
        ));
        assert!(client.snapshot().await.is_err());
        let state = client.state.lock().await;
        assert!(state.cached.is_none());
        assert!(state.next_request > Instant::now());
        drop(state);
        server.await.unwrap();
    }
}
