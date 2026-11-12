//! Offline paper fixtures only. No credentials, network, submit or cancel code.
use perp_arb_core::{decimal, Decimal};
use perp_arb_execution::{Engine, Leg, Plan, Side};
use perp_arb_private_events::{
    account::{Inventory, Snapshot},
    actor, OrderKey, OrderSnapshot, Scope, Status,
};
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
    io::{self, Read},
    time::{Duration, Instant},
};
fn scope(venue: &str) -> Scope {
    Scope {
        venue: venue.into(),
        account: "offline_fixture".into(),
    }
}
fn key(symbol: &str, id: &str) -> OrderKey {
    OrderKey {
        symbol: symbol.into(),
        order_id: id.into(),
    }
}
fn snapshot(
    venue: &str,
    symbol: &str,
    id: &str,
    original: Decimal,
    cumulative: Decimal,
    status: Status,
) -> OrderSnapshot {
    let now = Instant::now();
    OrderSnapshot {
        scope: scope(venue),
        key: key(symbol, id),
        original,
        cumulative,
        status,
        event_ms: Some(1),
        revision: 1,
        requires_reconciliation: false,
        observed_at: now,
        applied_at: now,
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut raw = String::new();
    io::stdin().take(1_048_577).read_to_string(&mut raw)?;
    if raw.len() > 1_048_576 {
        return Err("input limit".into());
    }
    let cases: Vec<Value> = serde_json::from_str(&raw)?;
    if cases.len() > 2000 {
        return Err("case limit".into());
    }
    let mut rows = Vec::new();
    for case in cases {
        let quantity = decimal(case["quantity"].as_str().ok_or("quantity")?)?;
        let filled = decimal(case["filled"].as_str().ok_or("filled")?)?;
        let status = match case["status"].as_str().ok_or("status")? {
            "FILLED" => Status::Filled,
            "CANCELED" => Status::Canceled,
            "REJECTED" => Status::Rejected,
            "EXPIRED" => Status::Expired,
            "NEW" => Status::New,
            "PARTIALLY_FILLED" => Status::PartiallyFilled,
            _ => return Err("status".into()),
        };
        let (m, mw) = actor::channel(scope("aster"), 8, 8)?;
        let (h, hw) = actor::channel(scope("hyperliquid"), 8, 8)?;
        let mt = mw.spawn();
        let ht = hw.spawn();
        for c in [&m, &h] {
            c.connected().await?;
            let ticket = c.begin_account_query().await?;
            let now = Instant::now();
            // Both fixtures are independently defined as empty; never query a real account.
            let inventory = Inventory {
                perp_dexes: BTreeSet::from([String::new()]),
                includes_wallets: true,
                ..Inventory::default()
            };
            c.apply_account_snapshot(
                ticket,
                Snapshot {
                    scope: c.scope().clone(),
                    inventory: inventory.clone(),
                    started_at: now,
                    completed_at: now,
                    exchange_time_ms: Some(1),
                },
                Some(inventory),
            )
            .await?;
        }
        let mr = m.account_report().await?.ok_or("maker report")?;
        let hr = h.account_report().await?.ok_or("hedge report")?;
        let plan = Plan {
            run_id: "fixture".into(),
            maker: Leg {
                scope: m.scope().clone(),
                symbol: "ETHUSDT".into(),
                side: Side::Sell,
            },
            hedge: Leg {
                scope: h.scope().clone(),
                symbol: "ETH".into(),
                side: Side::Buy,
            },
            maker_order: key("ETHUSDT", "1"),
            quantity,
            step: decimal("0.001")?,
            minimum: decimal("0.001")?,
        };
        let mut e = Engine::arm(
            plan,
            (&mr, &m.health()),
            (&hr, &h.health()),
            true,
            Duration::from_secs(5),
        )?;
        e.observe_maker(&snapshot(
            "aster",
            "ETHUSDT",
            "1",
            quantity,
            quantity,
            Status::Filled,
        ))?;
        let i = e.reserve_hedge()?.ok_or("intent")?;
        let mut hs = snapshot("hyperliquid", "ETH", "2", quantity, filled, status);
        hs.requires_reconciliation = case["gap"].as_bool().unwrap_or(false);
        e.observe_hedge(&i.id, &hs)?;
        e.observe_hedge(&i.id, &hs)?; // replay duplicate
        let retry = e.reserve_hedge()?.map_or(Decimal::ZERO, |i| i.quantity);
        rows.push(json!({"confirmed":e.confirmed_hedge()?.to_string(),"exposure":e.exposure()?.to_string(),"retry":retry.to_string()}));
        m.shutdown().await?;
        h.shutdown().await?;
        mt.await?;
        ht.await?;
    }
    println!("{}", serde_json::to_string(&rows)?);
    Ok(())
}
