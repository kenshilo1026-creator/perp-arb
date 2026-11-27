//! Read-only place_order selection policy. Venue support is deliberately not a
//! ranking criterion: unavailable execution must not silently change the trade.
use perp_arb_core::{decimal, Decimal};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

type Result<T> = std::result::Result<T, String>;
fn d(s: &str) -> Result<Decimal> {
    decimal(s).map_err(|_| "invalid decimal".into())
}
fn norm(s: &str) -> String {
    s.trim().to_ascii_lowercase()
}
fn venue(s: &str) -> Result<String> {
    let v = norm(s);
    if v.is_empty()
        || !v
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    {
        return Err("invalid venue".into());
    }
    Ok(v)
}
fn pair(s: &str, l: &str) -> Result<(String, String)> {
    let (s, l) = (venue(s)?, venue(l)?);
    if s == l {
        return Err("short and long venues must differ".into());
    }
    Ok((s, l))
}
fn mode(s: &str) -> Result<()> {
    if matches!(s, "open" | "close") {
        Ok(())
    } else {
        Err("mode must be open or close".into())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Signal {
    pub symbol: String,
    pub short_venue: String,
    pub long_venue: String,
    pub annualized_avg: String,
    pub signal: bool,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PairRequest {
    pub symbol: String,
    pub mode: String,
    pub short_venue: Option<String>,
    pub long_venue: Option<String>,
    pub enabled_venues: Vec<String>,
    pub updated_ms: Option<u64>,
    pub now_ms: u64,
    pub signals: Vec<Signal>,
}
#[derive(Debug, Serialize, PartialEq)]
pub struct PairChoice {
    pub symbol: String,
    pub short_venue: String,
    pub long_venue: String,
    pub annualized_avg: Option<String>,
    pub source: &'static str,
}
pub fn select_pair(r: &PairRequest) -> Result<PairChoice> {
    mode(&r.mode)?;
    let symbol = r.symbol.trim().to_ascii_uppercase();
    if symbol.is_empty() {
        return Err("symbol required".into());
    }
    let enabled: Vec<_> = r
        .enabled_venues
        .iter()
        .map(|v| venue(v))
        .collect::<Result<_>>()?;
    let s = r.short_venue.as_deref().map(venue).transpose()?;
    let l = r.long_venue.as_deref().map(venue).transpose()?;
    if r.mode == "close" && (s.is_none() || l.is_none()) {
        return Err("close requires explicit original short and long venues".into());
    }
    if let (Some(s), Some(l)) = (&s, &l) {
        let (s, l) = pair(s, l)?;
        // Closing an existing position remains possible after disabling new signals.
        if r.mode == "open" && (!enabled.contains(&s) || !enabled.contains(&l)) {
            return Err("explicit opening pair includes a disabled venue".into());
        }
        return Ok(PairChoice {
            symbol,
            short_venue: s,
            long_venue: l,
            annualized_avg: None,
            source: "explicit",
        });
    }
    let age = r.updated_ms.and_then(|t| r.now_ms.checked_sub(t));
    if !age.is_some_and(|age| age <= 86_400_000) {
        return Err(
            "monitor_signals timestamp invalid or older than 24 hours; run funding monitor first"
                .into(),
        );
    }
    let mut best: Option<(Decimal, String, String)> = None;
    for row in &r.signals {
        if !row.signal || row.symbol.trim().to_ascii_uppercase() != symbol {
            continue;
        }
        let Ok((rs, rl)) = pair(&row.short_venue, &row.long_venue) else {
            continue;
        };
        if !enabled.contains(&rs)
            || !enabled.contains(&rl)
            || s.as_ref().is_some_and(|s| s != &rs)
            || l.as_ref().is_some_and(|l| l != &rl)
        {
            continue;
        }
        let Ok(rate) = d(&row.annualized_avg) else {
            continue;
        };
        // Stable tie: preserve monitor order, as Python max() does.
        if best
            .as_ref()
            .is_none_or(|(previous, _, _)| rate > *previous)
        {
            best = Some((rate, rs, rl));
        }
    }
    let (rate, s, l) = best.ok_or("no eligible signal among enabled venues")?;
    Ok(PairChoice {
        symbol,
        short_venue: s,
        long_venue: l,
        annualized_avg: Some(rate.normalize().to_string()),
        source: "monitor",
    })
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Book {
    pub bid: String,
    pub ask: String,
}
impl Book {
    fn prices(&self) -> Result<(Decimal, Decimal)> {
        let (bid, ask) = (d(&self.bid)?, d(&self.ask)?);
        if bid <= Decimal::ZERO || bid > ask {
            return Err("orderbook anomaly detected".into());
        }
        Ok((bid, ask))
    }
    fn spread(&self) -> Result<Decimal> {
        let (b, a) = self.prices()?;
        a.checked_sub(b)
            .and_then(|v| v.checked_mul(Decimal::TWO))
            .and_then(|v| b.checked_add(a).and_then(|sum| v.checked_div(sum)))
            .ok_or_else(|| "spread arithmetic overflow".into())
    }
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegsRequest {
    pub short_venue: String,
    pub long_venue: String,
    pub mode: String,
    pub priorities: BTreeMap<String, i64>,
    pub books: BTreeMap<String, Book>,
    pub maker_venue: Option<String>,
    pub taker_venue: Option<String>,
}
#[derive(Debug, Serialize)]
pub struct LegsChoice {
    pub maker_venue: String,
    pub taker_venue: String,
    pub maker_side: &'static str,
    pub taker_side: &'static str,
    pub reduce_only: bool,
    pub maker_price: Option<String>,
    pub maker_price_source: &'static str,
    pub taker_reference_price: String,
    pub spread_by_venue: BTreeMap<String, String>,
    pub requires_spread_confirmation: bool,
    pub reason: &'static str,
    pub fee_model: &'static str,
    pub paired_execution_supported: bool,
}
pub fn select_legs(r: &LegsRequest) -> Result<LegsChoice> {
    mode(&r.mode)?;
    let (s, l) = pair(&r.short_venue, &r.long_venue)?;
    let sb = r.books.get(&s).ok_or("missing short orderbook")?;
    let lb = r.books.get(&l).ok_or("missing long orderbook")?;
    let (ss, ls) = (sb.spread()?, lb.spread()?);
    let threshold = d("0.001")?;
    let wide = ss > threshold || ls > threshold;
    let mo = r.maker_venue.as_deref().map(venue).transpose()?;
    let to = r.taker_venue.as_deref().map(venue).transpose()?;
    if mo.iter().chain(to.iter()).any(|v| v != &s && v != &l) || (mo.is_some() && mo == to) {
        return Err("maker/taker overrides must be distinct members of the selected pair".into());
    }
    let other = |v: &str| if v == s { l.clone() } else { s.clone() };
    let (maker, reason) = if let Some(m) = mo {
        (m, "explicit_override")
    } else if let Some(t) = to {
        (other(&t), "explicit_override")
    } else if s == "variational" || l == "variational" {
        ("variational".into(), "variational_maker")
    } else if wide {
        (if ls > ss { l.clone() } else { s.clone() }, "wider_spread")
    } else {
        let sp = r
            .priorities
            .get(&s)
            .ok_or_else(|| format!("missing execution priority for {s}"))?;
        let lp = r
            .priorities
            .get(&l)
            .ok_or_else(|| format!("missing execution priority for {l}"))?;
        if sp != lp {
            (
                if sp > lp { s.clone() } else { l.clone() },
                "configured_priority",
            )
        } else {
            (
                if ls > ss { l.clone() } else { s.clone() },
                "equal_priority_spread_tiebreak",
            )
        }
    };
    if (s == "variational" || l == "variational") && maker != "variational" {
        return Err("Variational must remain maker; browser taker is not supported".into());
    }
    let taker = other(&maker);
    let maker_buy = (maker == l) != (r.mode == "close");
    let (mb, ma) = r.books[&maker].prices()?;
    let (tb, ta) = r.books[&taker].prices()?;
    let browser = maker == "variational";
    Ok(LegsChoice {
        paired_execution_supported: (browser
            && matches!(taker.as_str(), "aster" | "hyperliquid" | "lighter"))
            || (maker != taker
                && [&maker, &taker]
                    .iter()
                    .all(|v| matches!(v.as_str(), "aster" | "hyperliquid" | "lighter"))),
        maker_venue: maker,
        taker_venue: taker,
        maker_side: if maker_buy { "BUY" } else { "SELL" },
        taker_side: if maker_buy { "SELL" } else { "BUY" },
        reduce_only: r.mode == "close",
        maker_price: if browser {
            None
        } else {
            Some(if maker_buy { mb } else { ma }.normalize().to_string())
        },
        maker_price_source: if browser {
            "variational_page_mid_button"
        } else if maker_buy {
            "best_bid"
        } else {
            "best_ask"
        },
        taker_reference_price: if maker_buy { tb } else { ta }.normalize().to_string(),
        spread_by_venue: BTreeMap::from([
            (s, ss.normalize().to_string()),
            (l, ls.normalize().to_string()),
        ]),
        requires_spread_confirmation: wide,
        reason,
        fee_model: "python_spread_and_configured_priority_no_dynamic_fees",
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn request() -> LegsRequest {
        serde_json::from_value(serde_json::json!({"short_venue":"aster","long_venue":"hyperliquid","mode":"open",
            "priorities":{"aster":2,"hyperliquid":1},"books":{"aster":{"bid":"99.99","ask":"100.01"},
            "hyperliquid":{"bid":"99.98","ask":"100.02"}}})).unwrap()
    }
    #[test]
    fn threshold_priority_tie_and_close() {
        let mut r = request();
        assert_eq!(select_legs(&r).unwrap().maker_venue, "aster");
        r.books.get_mut("hyperliquid").unwrap().ask = "100.2".into();
        let x = select_legs(&r).unwrap();
        assert_eq!(
            (
                x.maker_venue.as_str(),
                x.maker_side,
                x.maker_price.as_deref()
            ),
            ("hyperliquid", "BUY", Some("99.98"))
        );
        r.mode = "close".into();
        let x = select_legs(&r).unwrap();
        assert_eq!(
            (
                x.maker_side,
                x.taker_side,
                x.maker_price.as_deref(),
                x.reduce_only
            ),
            ("SELL", "BUY", Some("100.2"), true)
        );
        r.books.insert(
            "aster".into(),
            Book {
                bid: "99.95".into(),
                ask: "100.05".into(),
            },
        );
        r.books
            .insert("hyperliquid".into(), r.books["aster"].clone());
        assert!(!select_legs(&r).unwrap().requires_spread_confirmation); // exactly 0.1%
        r.priorities.insert("hyperliquid".into(), 2);
        assert_eq!(select_legs(&r).unwrap().maker_venue, "aster");
    }
    #[test]
    fn overrides_and_invalid_inputs() {
        let mut r = request();
        r.taker_venue = Some("ASTER".into());
        assert_eq!(select_legs(&r).unwrap().maker_venue, "hyperliquid");
        r.maker_venue = Some("aster".into());
        assert!(select_legs(&r).is_err());
        r.maker_venue = Some("unknown".into());
        assert!(select_legs(&r).is_err());
        r.maker_venue = None;
        r.taker_venue = None;
        for bad in ["0", "-1", "NaN", "101"] {
            r.books.get_mut("aster").unwrap().bid = bad.into();
            assert!(select_legs(&r).is_err());
        }
        let mut r = request();
        r.priorities.clear();
        assert!(select_legs(&r).unwrap_err().contains("priority"));
    }
    #[test]
    fn signals_cover_all_enabled_pairs_and_never_hide_unavailable_execution() {
        let mut r: PairRequest = serde_json::from_value(serde_json::json!({"symbol":"eth","mode":"open",
            "enabled_venues":["aster","hyperliquid","variational"],"now_ms":100000000,"updated_ms":100000000,
            "signals":[{"symbol":"ETH","short_venue":"aster","long_venue":"hyperliquid","annualized_avg":"9","signal":true},
            {"symbol":"ETH","short_venue":"variational","long_venue":"aster","annualized_avg":"1","signal":true}]})).unwrap();
        assert_eq!(select_pair(&r).unwrap().long_venue, "hyperliquid");
        r.enabled_venues.retain(|v| v != "hyperliquid");
        assert_eq!(select_pair(&r).unwrap().short_venue, "variational");
        r.updated_ms = Some(0);
        assert!(select_pair(&r).is_err());
        r.updated_ms = Some(r.now_ms + 1);
        assert!(select_pair(&r).is_err());
        r.mode = "close".into();
        assert!(select_pair(&r).is_err());
        r.short_venue = Some("aster".into());
        r.long_venue = Some("hyperliquid".into());
        assert_eq!(select_pair(&r).unwrap().source, "explicit");
        r.mode = "open".into();
        assert!(select_pair(&r).is_err());
    }
}
