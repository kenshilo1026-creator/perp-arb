use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex as StdMutex,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};

struct Reply {
    status: u16,
    headers: String,
    body: String,
    delay: Duration,
    chunked: bool,
}
impl Reply {
    fn json(value: Value) -> Self {
        Self {
            status: 200,
            headers: String::new(),
            body: value.to_string(),
            delay: Duration::ZERO,
            chunked: false,
        }
    }
    fn status(status: u16, headers: &str) -> Self {
        Self {
            status,
            headers: headers.into(),
            ..Self::json(json!({}))
        }
    }
}
struct Server {
    url: String,
    count: Arc<AtomicUsize>,
    requests: Arc<StdMutex<Vec<(Instant, String)>>>,
    task: JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn serve(replies: Vec<Reply>) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/test", listener.local_addr().unwrap());
    let count = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(StdMutex::new(Vec::new()));
    let c = count.clone();
    let r = requests.clone();
    let task = tokio::spawn(async move {
        for reply in replies {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut raw = Vec::new();
            loop {
                let mut buf = [0_u8; 1024];
                let n = stream.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                raw.extend_from_slice(&buf[..n]);
                if let Some(end) = raw.windows(4).position(|s| s == b"\r\n\r\n") {
                    let header = String::from_utf8_lossy(&raw[..end]).to_ascii_lowercase();
                    let length: usize = header
                        .lines()
                        .find_map(|l| {
                            l.strip_prefix("content-length:")
                                .and_then(|v| v.trim().parse().ok())
                        })
                        .unwrap_or(0);
                    if raw.len() >= end + 4 + length {
                        break;
                    }
                }
            }
            c.fetch_add(1, Ordering::SeqCst);
            r.lock()
                .unwrap()
                .push((Instant::now(), String::from_utf8(raw).unwrap()));
            tokio::time::sleep(reply.delay).await;
            let length = if reply.chunked {
                "Transfer-Encoding: chunked\r\n".into()
            } else {
                format!("Content-Length: {}\r\n", reply.body.len())
            };
            let header = format!(
                "HTTP/1.1 {} Test\r\nConnection: close\r\n{}{}\r\n",
                reply.status, length, reply.headers
            );
            let _ = stream.write_all(header.as_bytes()).await;
            let body = if reply.chunked {
                format!("{:X}\r\n{}\r\n0\r\n\r\n", reply.body.len(), reply.body)
            } else {
                reply.body
            };
            let _ = stream.write_all(body.as_bytes()).await;
        }
    });
    Server {
        url,
        count,
        requests,
        task,
    }
}
fn hl() -> Value {
    json!({"collateralToken":0,"universe":[{"name":"ETH","szDecimals":4}]})
}
fn aster() -> Value {
    json!({"symbols":[{"symbol":"ETHUSDT"}]})
}
fn policy() -> HttpPolicy {
    HttpPolicy {
        minimum_spacing: Duration::from_millis(5),
        request_timeout: Duration::from_secs(2),
        total_timeout: Duration::from_secs(3),
        retry_backoff: Duration::from_millis(10),
        ..HttpPolicy::default()
    }
}
fn client(server: &Server, p: HttpPolicy) -> PublicHttp {
    let mut http = PublicHttp::new(p).unwrap();
    // Only tests can change endpoints; production has a closed public-only enum.
    Arc::get_mut(&mut http.0).unwrap().urls =
        [server.url.clone(), server.url.clone(), server.url.clone()];
    http
}
fn cache(http: PublicHttp) -> MetadataCache {
    MetadataCache::new(
        http,
        Source::Hyperliquid,
        CachePolicy {
            refresh_after: Duration::from_millis(100),
            max_age: Duration::from_millis(250),
            failure_retry: Duration::from_millis(20),
        },
    )
    .unwrap()
}
async fn wait_count(server: &Server, n: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while server.count.load(Ordering::SeqCst) < n {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test(start_paused = true)]
async fn weighted_rolling_window_and_cancelled_wait_do_not_overspend() {
    let b = Arc::new(Budget::new(40, Duration::from_millis(10)));
    b.acquire(20).await;
    b.acquire(20).await;
    let c = b.clone();
    let pending = tokio::spawn(async move {
        c.acquire(20).await;
    });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(59)).await;
    assert!(!pending.is_finished());
    pending.abort();
    let _ = pending.await;
    assert_eq!(b.state.lock().await.requests.len(), 2);
    let start = Instant::now();
    b.acquire(20).await;
    assert!(start.elapsed() >= Duration::from_millis(990));
    assert_eq!(b.state.lock().await.requests.len(), 2);
}
#[tokio::test(start_paused = true)]
async fn cooldown_only_extends_and_does_not_affect_other_venue() {
    let http = PublicHttp::new(policy()).unwrap();
    http.0.aster.defer(Duration::from_secs(90)).await;
    http.0.aster.defer(Duration::from_secs(1)).await;
    let start = Instant::now();
    http.0.hl.acquire(20).await;
    assert_eq!(start.elapsed(), Duration::ZERO);
    http.0.aster.acquire(1).await;
    assert_eq!(start.elapsed(), Duration::from_secs(90));
}
#[test]
fn retry_after_seconds_dates_and_invalid_are_distinct() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
    assert_eq!(
        retry_after(Some("90000"), now),
        Some(Duration::from_secs(90000))
    );
    assert_eq!(
        retry_after(
            Some(&httpdate::fmt_http_date(now + Duration::from_secs(61))),
            now
        ),
        Some(Duration::from_secs(61))
    );
    assert_eq!(
        retry_after(
            Some(&httpdate::fmt_http_date(now - Duration::from_secs(1))),
            now
        ),
        Some(Duration::ZERO)
    );
    for raw in [None, Some("bad"), Some("-2"), Some("1.5")] {
        assert_eq!(retry_after(raw, now), None);
    }
}
#[test]
fn configuration_and_catalog_identity_validation() {
    assert!(PublicHttp::new(HttpPolicy {
        hyperliquid_weight_per_minute: 19,
        ..policy()
    })
    .is_err());
    assert!(PublicHttp::new(HttpPolicy {
        max_attempts: 0,
        ..policy()
    })
    .is_err());
    for value in [
        json!({}),
        json!({"universe":[]}),
        json!({"universe":[{"name":"ETH"},{"name":"eth"}]}),
        json!({"universe":[{}]}),
        json!({"collateralToken":1,"universe":[{"name":"ETH"}]}),
    ] {
        assert!(validate_catalog(Source::Hyperliquid, &value).is_err());
    }
    assert!(validate_catalog(Source::Hyperliquid, &hl()).is_ok());
}
#[tokio::test]
async fn request_methods_body_no_credentials_and_cache_hits() {
    let server = serve(vec![Reply::json(hl()), Reply::json(aster())]).await;
    let http = client(&server, policy());
    let c = cache(http.clone());
    assert!(c.current().is_err());
    assert!(!c.health().usable);
    c.refresh(false).await.unwrap();
    c.refresh(false).await.unwrap();
    let (generation, instrument) = c.resolve("ETH", &SymbolMappings::default()).unwrap();
    assert_eq!(generation, 1);
    assert_eq!(instrument.asset_id, Some(0));
    assert_eq!(server.count.load(Ordering::SeqCst), 1);
    http.fetch(Source::AsterV3).await.unwrap();
    let reqs = server.requests.lock().unwrap();
    assert!(reqs[0].1.starts_with("POST /test"));
    assert!(reqs[0].1.ends_with("{\"type\":\"meta\"}"));
    assert!(reqs[1].1.starts_with("GET /test"));
    for (_, request) in reqs.iter() {
        let request = request.to_ascii_lowercase();
        assert!(!request.contains("authorization:"));
        assert!(!request.contains("x-mbx-apikey:"));
        assert!(!request.contains("cookie:"));
    }
}
#[tokio::test]
async fn overlapping_refreshes_publish_once() {
    let mut reply = Reply::json(hl());
    reply.delay = Duration::from_millis(80);
    let server = serve(vec![reply]).await;
    let c = cache(client(&server, policy()));
    let mut tasks = Vec::new();
    for _ in 0..20 {
        let c = c.clone();
        tasks.push(tokio::spawn(async move {
            c.refresh(true).await.unwrap().generation
        }));
    }
    for task in tasks {
        assert_eq!(task.await.unwrap(), 1);
    }
    assert_eq!(server.count.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn failed_refresh_invalidates_old_rules_and_failure_backoff_coalesces() {
    let server = serve(vec![
        Reply::json(hl()),
        Reply::status(400, ""),
        Reply::json(hl()),
    ])
    .await;
    let c = cache(client(&server, policy()));
    c.refresh(false).await.unwrap();
    assert_eq!(c.refresh(true).await.err(), Some(MetadataError::Http(400)));
    assert!(c.resolve("ETH", &SymbolMappings::default()).is_err());
    assert!(!c.health().usable);
    for _ in 0..10 {
        assert!(c.refresh(true).await.is_err());
    }
    assert_eq!(server.count.load(Ordering::SeqCst), 2);
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(c.refresh(false).await.unwrap().generation, 3);
    assert!(c.health().usable);
}
#[tokio::test]
async fn expiry_uses_monotonic_age_without_network_on_reads() {
    let server = serve(vec![Reply::json(hl())]).await;
    let c = cache(client(&server, policy()));
    c.refresh(false).await.unwrap();
    tokio::time::sleep(Duration::from_millis(260)).await;
    assert!(matches!(c.current(), Err(MetadataError::Unavailable(_))));
    assert!(!c.health().usable);
    assert_eq!(server.count.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn changed_precision_delisting_and_asset_ids_replace_atomically() {
    let changed = json!({"universe":[{"name":"NEW","szDecimals":2},{"name":"ETH","szDecimals":3}]});
    let delisted = json!({"universe":[{"name":"ETH","szDecimals":3,"isDelisted":true}]});
    let server = serve(vec![
        Reply::json(hl()),
        Reply::json(changed),
        Reply::json(delisted),
    ])
    .await;
    let c = cache(client(&server, policy()));
    c.refresh(false).await.unwrap();
    let maps = SymbolMappings::default();
    let old = c.resolve("ETH", &maps).unwrap();
    c.refresh(true).await.unwrap();
    let new = c.resolve("ETH", &maps).unwrap();
    assert!(new.0 > old.0);
    assert!(!c.is_current_generation(old.0));
    assert!(c.is_current_generation(new.0));
    assert_eq!(new.1.asset_id, Some(1));
    assert_ne!(new.1.limit.lot(), old.1.limit.lot());
    c.refresh(true).await.unwrap();
    assert!(c.resolve("ETH", &maps).is_err());
    assert!(!c.is_current_generation(new.0));
}

#[tokio::test]
async fn overlapping_failed_refreshes_share_one_error_and_request() {
    let mut reply = Reply::status(400, "");
    reply.delay = Duration::from_millis(80);
    let server = serve(vec![reply]).await;
    let c = cache(client(&server, policy()));
    let mut tasks = Vec::new();
    for _ in 0..20 {
        let c = c.clone();
        tasks.push(tokio::spawn(async move { c.refresh(true).await.err() }));
    }
    for task in tasks {
        assert_eq!(task.await.unwrap(), Some(MetadataError::Http(400)));
    }
    assert_eq!(server.count.load(Ordering::SeqCst), 1);
    assert_eq!(c.health().generation, 1);
}

#[tokio::test]
async fn retryable_statuses_obey_retry_after_before_success() {
    for status in [429, 503] {
        let server = serve(vec![
            Reply::status(status, "Retry-After: 1\r\n"),
            Reply::json(hl()),
        ])
        .await;
        let http = client(&server, policy());
        http.fetch(Source::Hyperliquid).await.unwrap();
        assert_eq!(http.request_attempts(Source::Hyperliquid), 2);
        let reqs = server.requests.lock().unwrap();
        assert!(reqs[1].0.duration_since(reqs[0].0) >= Duration::from_secs(1));
    }
}

#[tokio::test]
async fn exhausted_5xx_retries_are_counted_and_bounded() {
    let server = serve((0..4).map(|_| Reply::status(500, "")).collect()).await;
    let http = client(&server, policy());
    assert_eq!(
        http.fetch(Source::Hyperliquid).await.err(),
        Some(MetadataError::Http(500))
    );
    assert_eq!(server.count.load(Ordering::SeqCst), 3);
    assert_eq!(http.request_attempts(Source::Hyperliquid), 3);
}

#[tokio::test]
async fn connection_refusal_is_retried_within_budget_and_deadline() {
    let server = serve(vec![]).await;
    // Its task closes the listening socket without accepting any connection.
    tokio::time::sleep(Duration::from_millis(10)).await;
    let http = client(
        &server,
        HttpPolicy {
            request_timeout: Duration::from_millis(50),
            ..policy()
        },
    );
    assert!(matches!(
        http.fetch(Source::Hyperliquid).await,
        Err(MetadataError::Transport(_))
    ));
    assert_eq!(http.request_attempts(Source::Hyperliquid), 3);
}

#[tokio::test(start_paused = true)]
async fn overflowing_retry_after_fails_closed_without_panicking() {
    let delay = retry_after(Some("99999999999999999999999999999999"), SystemTime::now()).unwrap();
    let budget = Budget::new(100, Duration::from_millis(1));
    budget.defer(delay).await;
    assert!(
        tokio::time::timeout(Duration::from_secs(1), budget.acquire(1))
            .await
            .is_err()
    );
    assert!(budget.state.lock().await.requests.is_empty());
}

#[tokio::test]
async fn malformed_json_is_not_retried_as_transient_http() {
    let server = serve(vec![Reply {
        body: "{".into(),
        ..Reply::json(hl())
    }])
    .await;
    let http = client(&server, policy());
    assert!(matches!(
        http.fetch(Source::Hyperliquid).await,
        Err(MetadataError::Invalid(_))
    ));
    assert_eq!(http.request_attempts(Source::Hyperliquid), 1);
}

#[tokio::test]
async fn other_venue_can_fetch_while_aster_is_in_cooldown() {
    let server = serve(vec![
        Reply::status(429, "Retry-After: 100\r\n"),
        Reply::json(hl()),
    ])
    .await;
    let http = client(
        &server,
        HttpPolicy {
            max_attempts: 1,
            ..policy()
        },
    );
    assert_eq!(
        http.fetch(Source::AsterV3).await.err(),
        Some(MetadataError::Http(429))
    );
    tokio::time::timeout(Duration::from_millis(500), http.fetch(Source::Hyperliquid))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(http.request_attempts(Source::Hyperliquid), 1);
}
#[tokio::test]
async fn malformed_envelope_never_replaces_snapshot_as_healthy() {
    let server = serve(vec![Reply::json(hl()), Reply::json(json!({"code":-1}))]).await;
    let c = cache(client(&server, policy()));
    c.refresh(false).await.unwrap();
    assert!(matches!(
        c.refresh(true).await,
        Err(MetadataError::Invalid(_))
    ));
    assert!(!c.health().usable);
    assert_eq!(c.health().generation, 2);
}
#[tokio::test]
async fn rejected_instrument_does_not_hide_other_valid_updates() {
    let server = serve(vec![Reply::json(
        json!({"universe":[{"name":"BAD","szDecimals":9},{"name":"ETH","szDecimals":3}]}),
    )])
    .await;
    let c = cache(client(&server, policy()));
    c.refresh(false).await.unwrap();
    assert!(c.resolve("BAD", &SymbolMappings::default()).is_err());
    assert_eq!(
        c.resolve("ETH", &SymbolMappings::default())
            .unwrap()
            .1
            .asset_id,
        Some(1)
    );
}
#[tokio::test]
async fn cancelled_refresh_releases_singleflight_and_does_not_publish() {
    let mut slow = Reply::json(hl());
    slow.delay = Duration::from_millis(80);
    let server = serve(vec![slow, Reply::json(hl())]).await;
    let c = cache(client(&server, policy()));
    let cloned = c.clone();
    let task = tokio::spawn(async move { cloned.refresh(false).await });
    wait_count(&server, 1).await;
    task.abort();
    let _ = task.await;
    assert_eq!(c.health().generation, 0);
    assert!(c.current().is_err());
    assert_eq!(c.refresh(false).await.unwrap().generation, 1);
}
#[tokio::test]
async fn periodic_refresh_and_shutdown_have_no_orphan_requests() {
    let server = serve((0..8).map(|_| Reply::json(hl())).collect()).await;
    let c = cache(client(&server, policy()));
    let worker = c.clone();
    let (stop, rx) = watch::channel(false);
    let task = tokio::spawn(async move { worker.run(rx).await });
    wait_count(&server, 2).await;
    stop.send(true).unwrap();
    task.await.unwrap();
    let count = server.count.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(server.count.load(Ordering::SeqCst), count);
}
#[tokio::test]
async fn stopping_during_fetch_cancels_public_io() {
    let mut slow = Reply::json(hl());
    slow.delay = Duration::from_secs(1);
    let server = serve(vec![slow]).await;
    let c = cache(client(&server, policy()));
    let worker = c.clone();
    let (stop, rx) = watch::channel(false);
    let task = tokio::spawn(async move { worker.run(rx).await });
    wait_count(&server, 1).await;
    drop(stop);
    tokio::time::timeout(Duration::from_millis(100), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(c.health().generation, 0);
}
#[tokio::test]
async fn retry_after_429_is_shared_between_aster_versions() {
    let server = serve(vec![
        Reply::status(429, "Retry-After: 1\r\n"),
        Reply::json(aster()),
    ])
    .await;
    let http = client(
        &server,
        HttpPolicy {
            max_attempts: 1,
            ..policy()
        },
    );
    assert_eq!(
        http.fetch(Source::AsterV1).await.err(),
        Some(MetadataError::Http(429))
    );
    let cloned = http.clone();
    cloned.fetch(Source::AsterV3).await.unwrap();
    let reqs = server.requests.lock().unwrap();
    assert!(reqs[1].0.duration_since(reqs[0].0) >= Duration::from_secs(1));
}
#[tokio::test]
async fn missing_retry_after_and_ip_weight_headers_block_without_retry_storm() {
    for reply in [
        Reply::status(429, ""),
        Reply {
            headers: "X-MBX-USED-WEIGHT-1M: 120\r\n".into(),
            ..Reply::json(aster())
        },
    ] {
        let server = serve(vec![reply]).await;
        let http = client(
            &server,
            HttpPolicy {
                total_timeout: Duration::from_millis(100),
                max_attempts: 1,
                ..policy()
            },
        );
        let _ = http.fetch(Source::AsterV1).await;
        assert_eq!(
            http.fetch(Source::AsterV3).await.err(),
            Some(MetadataError::Timeout)
        );
        assert_eq!(server.count.load(Ordering::SeqCst), 1);
    }
}
#[tokio::test]
async fn ban_is_not_retried_and_deadline_does_not_shorten_server_cooldown() {
    let server = serve(vec![Reply::status(418, "Retry-After: 90000\r\n")]).await;
    let http = client(
        &server,
        HttpPolicy {
            total_timeout: Duration::from_millis(50),
            ..policy()
        },
    );
    assert_eq!(
        http.fetch(Source::AsterV3).await.err(),
        Some(MetadataError::Http(418))
    );
    assert_eq!(
        http.fetch(Source::AsterV1).await.err(),
        Some(MetadataError::Timeout)
    );
    assert!(
        http.0
            .aster
            .state
            .lock()
            .await
            .blocked_until
            .duration_since(Instant::now())
            > Duration::from_secs(89999)
    );
    assert_eq!(server.count.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn transient_errors_are_bounded_and_client_errors_not_retried() {
    let server = serve(vec![
        Reply::status(500, ""),
        Reply::status(502, ""),
        Reply::json(hl()),
    ])
    .await;
    assert!(client(&server, policy())
        .fetch(Source::Hyperliquid)
        .await
        .is_ok());
    assert_eq!(server.count.load(Ordering::SeqCst), 3);
    let server = serve(vec![Reply::status(400, ""), Reply::json(hl())]).await;
    assert_eq!(
        client(&server, policy())
            .fetch(Source::Hyperliquid)
            .await
            .err(),
        Some(MetadataError::Http(400))
    );
    assert_eq!(server.count.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn redirects_and_oversize_length_or_chunks_are_rejected() {
    for chunked in [false, true] {
        let server = serve(vec![Reply {
            chunked,
            body: "x".repeat(129),
            ..Reply::json(hl())
        }])
        .await;
        assert_eq!(
            client(
                &server,
                HttpPolicy {
                    max_body_bytes: 128,
                    ..policy()
                }
            )
            .fetch(Source::Hyperliquid)
            .await
            .err(),
            Some(MetadataError::BodyTooLarge)
        );
    }
    let server = serve(vec![Reply::status(
        302,
        "Location: https://example.com/\r\n",
    )])
    .await;
    assert_eq!(
        client(&server, policy())
            .fetch(Source::Hyperliquid)
            .await
            .err(),
        Some(MetadataError::Http(302))
    );
}
#[tokio::test]
async fn slow_response_respects_total_deadline() {
    let mut slow = Reply::json(hl());
    slow.delay = Duration::from_secs(1);
    let server = serve(vec![slow]).await;
    assert_eq!(
        client(
            &server,
            HttpPolicy {
                total_timeout: Duration::from_millis(50),
                ..policy()
            }
        )
        .fetch(Source::Hyperliquid)
        .await
        .err(),
        Some(MetadataError::Timeout)
    );
    assert_eq!(server.count.load(Ordering::SeqCst), 1);
}
