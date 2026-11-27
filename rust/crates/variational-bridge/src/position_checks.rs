//! Selected-symbol baseline and post-fill deltas, matching Python execution scope.
use crate::Result;
use perp_arb_core::{decimal, Decimal as D};
use perp_arb_private_events::account::Inventory;
use serde::{Deserialize, Serialize};

pub fn quantity(i: &Inventory, symbol: &str) -> Result<D> {
    if i.positions
        .keys()
        .any(|k| k.symbol == symbol && k.side != "BOTH")
    {
        return Err("selected symbol requires one-way positions".into());
    }
    Ok(i.positions
        .iter()
        .find(|(k, _)| k.symbol == symbol)
        .map_or(D::ZERO, |(_, q)| *q))
}
fn tolerance(q: D) -> D {
    (q * decimal("0.001").unwrap()).max(decimal("0.00000001").unwrap())
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Baseline {
    pub maker: String,
    pub hedge: String,
}
impl Baseline {
    /// Position endpoints can lag confirmed fills. Retry reads only, never orders.
    pub async fn verify<F, Fut>(
        &self,
        maker_side: &str,
        maker_filled: D,
        hedge_filled: D,
        mut read: F,
    ) -> Result<()>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<Self>>,
    {
        for attempt in 0..3 {
            let result = match read().await {
                Ok(current) => self.after_fill(&current, maker_side, maker_filled, hedge_filled),
                Err(error) => Err(error),
            };
            if result.is_ok() || attempt == 2 {
                return result;
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
        unreachable!()
    }
    pub fn read_positions(m: &Inventory, ms: &str, h: &Inventory, hs: &str) -> Result<Self> {
        Ok(Self {
            maker: quantity(m, ms)?.to_string(),
            hedge: quantity(h, hs)?.to_string(),
        })
    }
    pub fn before_order(
        &self,
        q: D,
        maker_side: &str,
        hedge_side: &str,
        close: bool,
    ) -> Result<()> {
        let m = decimal(&self.maker).map_err(|e| e.to_string())?;
        let h = decimal(&self.hedge).map_err(|e| e.to_string())?;
        if (m + h).abs() > tolerance(q) {
            return Err(format!(
                "selected-symbol positions unbalanced before maker: maker={m} hedge={h}"
            ));
        }
        if close {
            for (p, s) in [(m, maker_side), (h, hedge_side)] {
                let available = if s == "BUY" { -p } else { p };
                if available < q {
                    return Err("close quantity/direction exceeds selected live position".into());
                }
            }
        }
        Ok(())
    }
    pub fn after_fill(
        &self,
        current: &Self,
        maker_side: &str,
        maker_filled: D,
        hedge_filled: D,
    ) -> Result<()> {
        let d = |s: &str| decimal(s).map_err(|e| e.to_string());
        let sign = if maker_side == "BUY" { D::ONE } else { -D::ONE };
        let md = (d(&current.maker)? - d(&self.maker)?) * sign;
        let hd = (d(&current.hedge)? - d(&self.hedge)?) * -sign;
        let tol = tolerance(maker_filled.max(hedge_filled));
        if (md - maker_filled).abs() > tol || (hd - hedge_filled).abs() > tol {
            return Err(format!("position delta differs from fills: maker_delta={md} maker_filled={maker_filled} hedge_delta={hd} hedge_filled={hedge_filled}; inspect/recover same run"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn d(s: &str) -> D {
        decimal(s).unwrap()
    }
    fn baseline(m: &str, h: &str) -> Baseline {
        Baseline {
            maker: m.into(),
            hedge: h.into(),
        }
    }
    #[test]
    fn baseline_and_deltas_match_selected_symbol_python_policy() {
        let b = baseline("-20", "20");
        assert!(b.before_order(d("2"), "BUY", "SELL", true).is_ok());
        assert!(b.before_order(d("21"), "BUY", "SELL", true).is_err());
        assert!(b.before_order(d("2"), "SELL", "BUY", true).is_err());
        assert!(baseline("-20", "19")
            .before_order(d("2"), "BUY", "SELL", false)
            .is_err());
        assert!(b
            .after_fill(&baseline("-18", "18"), "BUY", d("2"), d("2"))
            .is_ok());
        assert!(b
            .after_fill(&baseline("-18", "20"), "BUY", d("2"), d("2"))
            .is_err());
    }
    #[tokio::test]
    async fn delayed_position_visibility_retries_reads_only() {
        let mut calls = 0;
        baseline("0", "0")
            .verify("BUY", d("1"), d("1"), || {
                calls += 1;
                let current = if calls == 1 {
                    baseline("0", "0")
                } else {
                    baseline("1", "-1")
                };
                async move { Ok(current) }
            })
            .await
            .unwrap();
        assert_eq!(calls, 2);
        let mut calls = 0;
        assert!(baseline("0", "0")
            .verify("BUY", d("1"), d("1"), || {
                calls += 1;
                async { Ok(baseline("1", "0")) }
            })
            .await
            .is_err());
        assert_eq!(calls, 3);
    }
}
