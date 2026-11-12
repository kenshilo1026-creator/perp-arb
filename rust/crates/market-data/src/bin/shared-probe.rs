//! Short public-only smoke test of shared BBO/depth connections. No raw recording.
use perp_arb_market_data::{
    hub::{wall_ms, MarketHub, Subscription},
    shared::MarketKey,
    Venue,
};
use serde_json::json;
use std::{fs::OpenOptions, io::Write, path::PathBuf, time::Duration};
use tokio::time::{sleep_until, Instant};

#[derive(Default)]
struct Stats {
    notifications: u64,
    samples: u64,
    fresh: u64,
    unavailable: u64,
}
impl Stats {
    fn sample(&mut self, sub: &Subscription) {
        self.samples += 1;
        if sub.snapshot().fresh(wall_ms()).is_some() {
            self.fresh += 1;
        } else {
            self.unavailable += 1;
        }
    }
    fn json(&self, sub: &Subscription, venue: &str) -> serde_json::Value {
        let view = sub.snapshot();
        json!({"venue":venue,"market_generation":view.generation,"accepted_depth":view.accepted_depth,
            "accepted_bbo":view.accepted_bbo,"watch_notifications":self.notifications,"timer_samples":self.samples,
            "fresh_samples":self.fresh,"unavailable_samples":self.unavailable,"last_reason":view.reason})
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let mut output = None;
    let mut seconds = 30;
    while let Some(option) = args.next() {
        let value = args.next().ok_or("each option needs a value")?;
        match option.as_str() {
            "--output" => output = Some(PathBuf::from(value)),
            "--seconds" => seconds = value.parse::<u64>()?,
            _ => return Err(format!("unknown option {option}").into()),
        }
    }
    if !(1..=300).contains(&seconds) {
        return Err("seconds must be 1..300".into());
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output.ok_or("--output required; file must not exist")?)?;
    let mut hub = MarketHub::new();
    let a = MarketKey::new(Venue::Aster, "ETHUSDT")?;
    let h = MarketKey::new(Venue::Hyperliquid, "ETH")?;
    let mut aster = hub.subscribe(a.clone()).await?;
    let mut hyperliquid = hub.subscribe(h.clone()).await?;
    let _aster_second_consumer = hub.subscribe(a).await?;
    let _hl_second_consumer = hub.subscribe(h).await?;
    let mut stats = [Stats::default(), Stats::default()];
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let mut timer = tokio::time::interval(Duration::from_millis(100));
    let mut failure = None;
    loop {
        tokio::select! {
            biased;
            _=sleep_until(deadline)=>break,
            _=timer.tick()=>{stats[0].sample(&aster);stats[1].sample(&hyperliquid);},
            value=aster.changed()=>{
                if let Err(e)=value {failure=Some(e);break;}
                stats[0].notifications+=1;
            },
            value=hyperliquid.changed()=>{
                if let Err(e)=value {failure=Some(e);break;}
                stats[1].notifications+=1;
            },
        }
    }
    let venues = [
        stats[0].json(&aster, "aster"),
        stats[1].json(&hyperliquid, "hyperliquid"),
    ];
    let complete = failure.is_none()
        && venues.iter().all(|v| {
            v["accepted_depth"].as_u64().unwrap_or(0) > 0
                && v["accepted_bbo"].as_u64().unwrap_or(0) > 0
                && v["fresh_samples"].as_u64().unwrap_or(0) > 0
        });
    let transports = [hub.stats(Venue::Aster), hub.stats(Venue::Hyperliquid)];
    hub.shutdown().await?;
    let cleared = aster.snapshot().selected(wall_ms()).is_none()
        && hyperliquid.snapshot().selected(wall_ms()).is_none();
    let report = json!({"complete":complete && cleared,"seconds":seconds,"public_only":true,
        "consumers":4,"venues":venues,"transports":transports,"shutdown_cleared":cleared,"error":failure,
        "scope":"Connection smoke test; watch notifications may coalesce. No fill latency or profitability measurement."});
    let text = serde_json::to_string_pretty(&report)?;
    writeln!(file, "{text}")?;
    file.flush()?;
    println!("{text}");
    if !complete || !cleared {
        return Err("shared probe incomplete; inspect venue counts and reasons".into());
    }
    Ok(())
}
