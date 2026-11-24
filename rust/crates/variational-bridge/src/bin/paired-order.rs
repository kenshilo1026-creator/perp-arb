use perp_arb_variational_bridge::{
    paired::{self, Action, Baseline, Plan},
    Result,
};
use std::path::PathBuf;
const HELP:&str="Rust Variational + Aster/Hyperliquid 雙腿委託\npaired-order preview --plan PLAN.json\npaired-order audit --plan PLAN.json --baseline-output NEW.json\npaired-order submit --plan PLAN.json --baseline REVIEWED.json --run-dir NEW_DIR --live\npaired-order inspect --run-dir DIR\npaired-order recover --run-dir DIR --live\n\nVariational 固定限價，對沖腿為明確限價的 IOC；不自動重掛 Maker。\n平倉：兩腿 reduce_only=true，方向與既有持倉相反。\n先核對 audit 的庫存基準，再將 reviewed 設為 true；送單時會重新讀取比對。\n回覆不明不重送。未完成時使用同一 run-dir recover，不刪除帳本。\n金鑰只讀程序環境；Python wrapper 可使用現有 .env。\n詳細操作：rust/PAIRED_ORDER.md";
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
        println!(
            "paired-order mid-price --extension-id CHROME_ID --symbol ETH (Limit/Mid preview only)"
        );
        return Ok(());
    }
    let command = args.first().ok_or("use --help")?;
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
    let result = match command.as_str() {
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
            let b = Baseline::read(&baseline.ok_or("--baseline required")?, &p)?;
            let dir = run.ok_or("--run-dir required")?;
            if dir.exists() {
                return Err("run directory exists; inspect/recover it, never resubmit".into());
            }
            paired::execute(Action::Submit, p, dir, Some(b), parent_control).await?
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
