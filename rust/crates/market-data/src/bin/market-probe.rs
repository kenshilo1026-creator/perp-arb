use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args().skip(1);
    let mut output = None;
    let mut seconds = 30;
    let mut aster = "ETHUSDT".to_string();
    let mut hyperliquid = "ETH".to_string();
    while let Some(option) = arguments.next() {
        let value = arguments.next().ok_or("each option needs a value")?;
        match option.as_str() {
            "--output" => output = Some(PathBuf::from(value)),
            "--seconds" => seconds = value.parse()?,
            "--aster-symbol" => aster = value,
            "--hyperliquid-coin" => hyperliquid = value,
            _ => return Err(format!("unknown option {option}").into()),
        }
    }
    let output = output.ok_or(
        "--output is required; parent directory must exist; existing files are never overwritten",
    )?;
    let summary =
        perp_arb_market_data::recorder::record(&output, seconds, aster, hyperliquid).await?;
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}
