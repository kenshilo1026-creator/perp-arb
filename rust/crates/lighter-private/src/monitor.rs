//! Authenticated order stream plus independent REST reconciliation. Reconnects invalidate risk readiness.
use crate::{decode_order, Client, Result};
use futures_util::{SinkExt, StreamExt};
use perp_arb_private_events::{actor::Coordinator, lookup::Outcome, Evidence};
use serde_json::{json, Value};
use std::time::{Duration, Instant};
use tokio::{sync::watch, task::JoinHandle};
use tokio_tungstenite::tungstenite::Message;
pub struct Monitor {
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<Result<()>>>,
}
impl Drop for Monitor {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}
impl Monitor {
    pub async fn shutdown(mut self) -> Result<()> {
        self.stop.send_replace(true);
        self.task
            .take()
            .ok_or("monitor closed")?
            .await
            .map_err(|_| "Lighter monitor failed")?
    }
}
pub async fn start(client: Client, c: Coordinator) -> Result<Monitor> {
    if c.scope() != &client.scope() {
        return Err("Lighter monitor scope".into());
    }
    let lease = c.acquire_transport().map_err(|e| e.to_string())?;
    c.transport_lost().await.map_err(|e| e.to_string())?;
    let (stop, rx) = watch::channel(false);
    let task = tokio::spawn(async move {
        let _lease = lease;
        let mut stopped = rx.clone();
        let work = async {
            let mut backoff = 1;
            loop {
                let session = c.connecting().await.map_err(|e| e.to_string())?;
                let mut poll = Box::pin(poll(client.clone(), c.clone()));
                let result =
                    tokio::select! {r=socket(&client,&c,session.clone())=>r,r=&mut poll=>r};
                c.disconnected(session).await.map_err(|e| e.to_string())?;
                if let Err(e) = result {
                    eprintln!("[fill-stream] lighter reconnect: {e}");
                }
                tokio::time::sleep(Duration::from_secs(backoff)).await;
                backoff = (backoff * 2).min(30);
            }
        };
        let result = tokio::select! {r=work=>r,_=stopped.changed()=>Ok(())};
        c.transport_lost().await.map_err(|e| e.to_string())?;
        result
    });
    Ok(Monitor {
        stop,
        task: Some(task),
    })
}
async fn socket(
    client: &Client,
    c: &Coordinator,
    session: perp_arb_private_events::Session,
) -> Result<()> {
    let (mut ws, _) = tokio::time::timeout(
        Duration::from_secs(10),
        tokio_tungstenite::connect_async(perp_arb_market_data::lighter::WS),
    )
    .await
    .map_err(|_| "private WS timeout")?
    .map_err(|_| "private WS connection")?;
    let auth = client.auth()?;
    for channel in ["account_all_orders", "account_all"] {
        ws.send(Message::Text(json!({"type":"subscribe","channel":format!("{channel}/{}",client.0.account),"auth":auth}).to_string().into())).await.map_err(|_|"private subscription failed")?;
    }
    let mut orders_ready = false;
    let mut account_ready = false;
    let began = Instant::now();
    let mut last_frame = Instant::now();
    let mut heartbeat = tokio::time::interval(Duration::from_secs(15));
    heartbeat.tick().await;
    while began.elapsed() < Duration::from_secs(480) {
        // A quiet account has no order messages. Probe transport liveness rather
        // than repeatedly disconnecting a healthy stream because nobody traded.
        let msg = tokio::select! {
            msg=ws.next()=>msg.ok_or("private WS closed")?.map_err(|_|"private WS transport")?,
            _=heartbeat.tick()=>{
                if last_frame.elapsed()>Duration::from_secs(45){return Err("private WS heartbeat timeout".into());}
                ws.send(Message::Ping(Vec::new().into())).await.map_err(|_|"private WS heartbeat send")?;
                continue;
            }
        };
        last_frame = Instant::now();
        let received = Instant::now();
        let Message::Text(raw) = msg else {
            if msg.is_close() {
                return Err("private WS closed".into());
            }
            continue;
        };
        if raw.len() > 4_000_000 {
            return Err("private WS capacity".into());
        }
        let v: Value = serde_json::from_str(&raw).map_err(|_| "private WS JSON")?;
        let kind = v["type"].as_str().ok_or("private WS message type")?;
        if kind == "connected" {
            continue;
        }
        if kind == "ping" {
            ws.send(Message::Text("{\"type\":\"pong\"}".into()))
                .await
                .map_err(|_| "private pong")?;
            continue;
        }
        if kind.ends_with("/account_all_orders")
            && v["channel"] == format!("account_all_orders:{}", client.0.account)
        {
            let groups = v["orders"]
                .as_object()
                .ok_or("missing private order groups")?;
            for (market, rows) in groups {
                let market: u64 = market.parse().map_err(|_| "private market ID")?;
                // Other markets invalidate account evidence; only this client's native market
                // can be decoded into execution events without guessing symbol mappings.
                if Some(market as usize) != client.0.spec.asset_id {
                    c.account_changed(session.clone())
                        .await
                        .map_err(|e| e.to_string())?;
                    continue;
                }
                for row in rows.as_array().ok_or("private order array")? {
                    let f = decode_order(row, &client.scope(), client.0.spec.key.symbol(), market)?;
                    client.remember(f.clone())?;
                    c.observe(f.order, Evidence::Ws(session.clone()), received)
                        .await
                        .map_err(|e| e.to_string())?;
                }
            }
            orders_ready |= kind == "subscribed/account_all_orders";
        } else if kind.ends_with("/account_all")
            && v["channel"] == format!("account_all:{}", client.0.account)
        {
            c.account_changed(session.clone())
                .await
                .map_err(|e| e.to_string())?;
            account_ready |= kind == "subscribed/account_all";
        } else {
            return Err("private WS error or unexpected channel".into());
        }
        if orders_ready && account_ready {
            c.activate(session.clone())
                .await
                .map_err(|e| e.to_string())?;
            orders_ready = false;
            account_ready = false;
        }
    }
    Ok(()) // renew token with a fresh connection before it expires
}
async fn poll(client: Client, c: Coordinator) -> Result<()> {
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let requests: Vec<_> = client
            .0
            .tracked
            .lock()
            .map_err(|_| "tracked poisoned")?
            .values()
            .cloned()
            .collect();
        for request in requests {
            let f = client
                .0
                .found
                .lock()
                .map_err(|_| "cache poisoned")?
                .get(match &request.selector {
                    perp_arb_private_events::lookup::Selector::ClientId(s) => s,
                    _ => continue,
                })
                .cloned();
            let Some(f) = f else {
                continue;
            };
            let snapshots = c.snapshots().await.map_err(|e| e.to_string())?;
            let Some(snapshot) = snapshots.iter().find(|s| s.key == f.order.key) else {
                continue;
            };
            if snapshot.status.terminal() && !snapshot.requires_reconciliation {
                continue;
            }
            let ticket = c
                .begin_query(f.order.key)
                .await
                .map_err(|e| e.to_string())?;
            if let Ok(Outcome::Found(found)) = client.lookup(&request).await {
                c.observe(found.order, Evidence::Rest(ticket), Instant::now())
                    .await
                    .map_err(|e| e.to_string())?;
            }
        }
    }
}
