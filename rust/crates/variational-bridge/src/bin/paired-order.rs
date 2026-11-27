use perp_arb_variational_bridge::{
    paired::{self, Action, Baseline, Plan},
    Result,
};
use std::path::PathBuf;
const HELP:&str="Rust Aster / Hyperliquid / Lighter / Variational 雙腿委託\npaired-order preview --plan PLAN.json\npaired-order audit --plan PLAN.json --baseline-output NEW.json\npaired-order submit --plan PLAN.json --run-dir NEW_DIR --live\npaired-order inspect --run-dir DIR\npaired-order recover --run-dir DIR --live\n\nVariational 固定限價，對沖腿依計劃使用 Aster MARKET 或明確限價的 IOC；不自動重掛 Maker。\n平倉：兩腿 reduce_only=true，方向與既有持倉相反。\nSubmit checks only the selected positions; audit is optional diagnostics.\n回覆不明不重送。未完成時使用同一 run-dir recover，不刪除帳本。\n金鑰只讀程序環境；Python wrapper 可使用現有 .env。\n詳細操作：rust/PAIRED_ORDER.md；Lighter：rust/LIGHTER.md";
#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("paired-order: {e}");
        std::process::exit(1)
    }
}
async fn run() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args == ["--help"] {
        println!("{HELP}");
        println!("原生 Aster↔Hyperliquid：native_perp_pair_v1，最新 bid/ask Maker、部分成交 WS 對沖及撤單確認後重掛；不需要 extension。");
        println!(
            "paired-order mid-price --extension-id CHROME_ID --symbol ETH (Limit/Mid preview only)"
        );
        println!("paired-order select-pair | select-legs (read-only JSON input on stdin; no credentials)");
        return Ok(());
    }
    let command = args.first().ok_or("use --help")?;
    if matches!(command.as_str(), "select-pair" | "select-legs") {
        use perp_arb_execution::selection::{select_legs, select_pair};
        use std::io::Read;
        if args.len() != 1 {
            return Err("selection takes only JSON stdin; no trading flags".into());
        }
        let mut input = String::new();
        std::io::stdin()
            .take(4_194_305)
            .read_to_string(&mut input)
            .map_err(|_| "selection input")?;
        if input.len() > 4_194_304 {
            return Err("selection input too large".into());
        }
        let input: serde_json::Value =
            serde_json::from_str(&input).map_err(|_| "invalid selection JSON")?;
        let select = |value: serde_json::Value| -> Result<serde_json::Value> {
            if command == "select-pair" {
                let request = serde_json::from_value(value)
                    .map_err(|e| format!("invalid pair request: {e}"))?;
                serde_json::to_value(select_pair(&request)?)
                    .map_err(|_| "pair serialization".into())
            } else {
                let request = serde_json::from_value(value)
                    .map_err(|e| format!("invalid legs request: {e}"))?;
                serde_json::to_value(select_legs(&request)?)
                    .map_err(|_| "legs serialization".into())
            }
        };
        let selection = if let serde_json::Value::Array(values) = input {
            serde_json::Value::Array(values.into_iter().map(select).collect::<Result<Vec<_>>>()?)
        } else {
            select(input)?
        };
        println!(
            "{}",
            serde_json::json!({"mode":command,"selection":selection,"can_submit_orders":false})
        );
        return Ok(());
    }
    if command == "mid-price" {
        let mut extension = None;
        let mut symbol = None;
        let mut options = args[1..].iter();
        while let Some(key) = options.next() {
            match key.as_str() {
                "--extension-id" if extension.is_none() => {
                    extension = Some(options.next().ok_or("missing extension ID")?.clone())
                }
                "--symbol" if symbol.is_none() => {
                    symbol = Some(options.next().ok_or("missing symbol")?.clone())
                }
                _ => {
                    return Err(
                        "mid-price only accepts --extension-id and --symbol; no trading options"
                            .into(),
                    )
                }
            }
        }
        let result = perp_arb_variational_bridge::mid_preview::run(
            perp_arb_variational_bridge::Config::new(extension.ok_or("--extension-id required")?),
            &symbol.ok_or("--symbol required")?,
        )
        .await?;
        println!(
            "{}",
            serde_json::to_string(&result).map_err(|_| "Mid preview serialization")?
        );
        return Ok(());
    }
    let mut plan = None;
    let mut run = None;
    let mut baseline = None;
    let mut output = None;
    let mut live = false;
    let mut parent_control = false;
    let mut it = args[1..].iter();
    while let Some(k) = it.next() {
        match k.as_str() {
            "--live" if !live => live = true,
            "--parent-control" if !parent_control => parent_control = true,
            "--plan" if plan.is_none() => {
                plan = Some(PathBuf::from(it.next().ok_or("missing plan")?))
            }
            "--run-dir" if run.is_none() => {
                run = Some(PathBuf::from(it.next().ok_or("missing run directory")?))
            }
            "--baseline" if baseline.is_none() => {
                baseline = Some(PathBuf::from(it.next().ok_or("missing baseline")?))
            }
            "--baseline-output" if output.is_none() => {
                output = Some(PathBuf::from(it.next().ok_or("missing output")?))
            }
            _ => return Err("unknown or duplicate option".into()),
        }
    }
    if parent_control && (!live || !matches!(command.as_str(), "submit" | "recover")) {
        return Err("parent control only applies to live submit/recover".into());
    }
    let source = plan
        .clone()
        .or_else(|| run.as_ref().map(|r| r.join("plan.json")));
    let native = source
        .as_ref()
        .and_then(|p| std::fs::read(p).ok())
        .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
        .is_some_and(|v| v["schema"] == "native_perp_pair_v1");
    if native {
        use perp_arb_variational_bridge::native_pair as n;
        let result = match command.as_str() {
            "preview" if !live && run.is_none() && baseline.is_none() && output.is_none() => {
                n::preview(&n::Plan::read(&plan.ok_or("plan required")?)?).await?
            }
            "audit" if !live && run.is_none() && baseline.is_none() => {
                n::execute(
                    Action::Audit,
                    n::Plan::read(&plan.ok_or("plan required")?)?,
                    output.ok_or("baseline output required")?,
                    None,
                    false,
                )
                .await?
            }
            "submit" if live && output.is_none() => {
                n::execute(
                    Action::Submit,
                    n::Plan::read(&plan.ok_or("plan required")?)?,
                    run.ok_or("run directory required")?,
                    baseline,
                    parent_control,
                )
                .await?
            }
            "recover" if live && plan.is_none() && baseline.is_none() && output.is_none() => {
                let dir = run.ok_or("run directory required")?;
                n::execute(
                    Action::Recover,
                    n::Plan::read(&dir.join("plan.json"))?,
                    dir,
                    None,
                    parent_control,
                )
                .await?
            }
            "inspect" if !live && plan.is_none() && baseline.is_none() && output.is_none() => {
                n::inspect(&run.ok_or("run directory required")?).await?
            }
            _ => return Err("invalid native pair action/options; see --help".into()),
        };
        println!(
            "{}",
            serde_json::to_string_pretty(&result).map_err(|e| e.to_string())?
        );
        return Ok(());
    }
    let result = match command.as_str() {
        "ui-prepare" if !live && run.is_none() && baseline.is_none() && output.is_none() => {
            let p = Plan::read(&plan.ok_or("--plan required")?)?;
            perp_arb_variational_bridge::mid_preview::prepare_ticket(&p).await?
        }
        "preview" if !live && run.is_none() && baseline.is_none() && output.is_none() => {
            paired::preview(&Plan::read(&plan.ok_or("--plan required")?)?).await?
        }
        "audit" if !live && run.is_none() && baseline.is_none() => {
            paired::execute(
                Action::Audit,
                Plan::read(&plan.ok_or("--plan required")?)?,
                output.ok_or("--baseline-output required")?,
                None,
                false,
            )
            .await?
        }
        "submit" if live && output.is_none() => {
            let p = Plan::read(&plan.ok_or("--plan required")?)?;
            let b = baseline
                .as_ref()
                .map(|path| Baseline::read(path, &p))
                .transpose()?;
            let dir = run.ok_or("--run-dir required")?;
            if dir.exists() {
                return Err("run directory exists; inspect/recover it, never resubmit".into());
            }
            paired::execute(Action::Submit, p, dir, b, parent_control).await?
        }
        "recover" if live && plan.is_none() && baseline.is_none() && output.is_none() => {
            let dir = run.ok_or("--run-dir required")?;
            let p = Plan::read(&dir.join("plan.json"))?;
            paired::execute(Action::Recover, p, dir, None, parent_control).await?
        }
        "inspect" if !live && plan.is_none() && baseline.is_none() && output.is_none() => {
            paired::inspect(&run.ok_or("--run-dir required")?).await?
        }
        _ => {
            return Err("invalid action/options; submit/recover require --live; see --help".into())
        }
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&result).map_err(|_| "result serialization")?
    );
    Ok(())
}
