//! Offline JSON harness. It has no exchange connector or live trading mode.
#![forbid(unsafe_code)]

use perp_arb_core::{
    decimal,
    market::{vwap, Depth, Freshness, FreshnessPolicy, Level, Quote},
    pricing::{adverse_price_gap, spread_bps, Direction, RoundingInput, Side},
    Decimal, Result,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    hint::black_box,
    io::{self, Read},
    time::Instant,
};

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
enum InputSide {
    Buy,
    Sell,
}
impl From<InputSide> for Side {
    fn from(value: InputSide) -> Self {
        match value {
            InputSide::Buy => Self::Buy,
            InputSide::Sell => Self::Sell,
        }
    }
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum InputDirection {
    Up,
    Down,
}
impl From<InputDirection> for Direction {
    fn from(value: InputDirection) -> Self {
        match value {
            InputDirection::Up => Self::Up,
            InputDirection::Down => Self::Down,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InputQuote {
    bid: String,
    ask: String,
    received_ms: i64,
    source_ms: Option<i64>,
    // null = intentionally unsized; [] = visible depth has no liquidity.
    #[serde(deserialize_with = "required_depth")]
    bids: Option<Vec<[String; 2]>>,
    #[serde(deserialize_with = "required_depth")]
    asks: Option<Vec<[String; 2]>>,
}

// Missing is not the same as explicitly unsized. Prevent malformed input from
// accidentally opting out of depth checks.
fn required_depth<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<Vec<[String; 2]>>, D::Error> {
    Option::<Vec<[String; 2]>>::deserialize(deserializer)
}

fn levels(rows: Vec<[String; 2]>) -> Result<Vec<Level>> {
    rows.into_iter()
        .map(|[price, size]| {
            Ok(Level {
                price: decimal(&price)?,
                size: decimal(&size)?,
            })
        })
        .collect()
}

impl InputQuote {
    fn prepare(self) -> Result<Quote> {
        fn depth(rows: Option<Vec<[String; 2]>>) -> Result<Depth> {
            match rows {
                None => Ok(Depth::Unsized),
                Some(rows) => Ok(Depth::Visible(levels(rows)?)),
            }
        }
        Quote::new(
            decimal(&self.bid)?,
            decimal(&self.ask)?,
            self.received_ms,
            self.source_ms,
            depth(self.bids)?,
            depth(self.asks)?,
        )
    }
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Input {
    Spread {
        short: String,
        long: String,
    },
    Adverse {
        maker: String,
        taker: String,
        side: InputSide,
    },
    Round {
        value: String,
        step: Option<String>,
        direction: InputDirection,
    },
    Vwap {
        levels: Vec<[String; 2]>,
        quantity: String,
    },
    Executable {
        quote: InputQuote,
        side: InputSide,
        quantity: Option<String>,
    },
    Marginal {
        quote: InputQuote,
        side: InputSide,
        quantity: String,
    },
    Fresh {
        quote: InputQuote,
        now_ms: i64,
        healthy: bool,
        policy_ms: [i64; 3],
    },
}

enum Operation {
    Spread(Decimal, Decimal),
    Adverse(Decimal, Decimal, Side),
    Round(RoundingInput, Option<Decimal>, Direction),
    Vwap(Vec<Level>, Decimal),
    Executable(Quote, Side, Option<Decimal>),
    Marginal(Quote, Side, Decimal),
    Fresh(Quote, i64, bool, FreshnessPolicy),
}

impl Input {
    fn prepare(self) -> Result<Operation> {
        Ok(match self {
            Self::Spread { short, long } => Operation::Spread(decimal(&short)?, decimal(&long)?),
            Self::Adverse { maker, taker, side } => {
                Operation::Adverse(decimal(&maker)?, decimal(&taker)?, side.into())
            }
            Self::Round {
                value,
                step,
                direction,
            } => Operation::Round(
                RoundingInput::parse(&value)?,
                step.as_deref().map(decimal).transpose()?,
                direction.into(),
            ),
            Self::Vwap {
                levels: rows,
                quantity,
            } => Operation::Vwap(levels(rows)?, decimal(&quantity)?),
            Self::Executable {
                quote,
                side,
                quantity,
            } => Operation::Executable(
                quote.prepare()?,
                side.into(),
                quantity.as_deref().map(decimal).transpose()?,
            ),
            Self::Marginal {
                quote,
                side,
                quantity,
            } => Operation::Marginal(quote.prepare()?, side.into(), decimal(&quantity)?),
            Self::Fresh {
                quote,
                now_ms,
                healthy,
                policy_ms: [age, lag, future],
            } => Operation::Fresh(
                quote.prepare()?,
                now_ms,
                healthy,
                FreshnessPolicy::new(age, lag, future)?,
            ),
        })
    }
}

enum Output {
    Number(Option<Decimal>),
    Boolean(bool),
}
impl Operation {
    fn evaluate(&self) -> Result<Output> {
        let value = match self {
            Self::Spread(s, l) => Some(spread_bps(*s, *l)?),
            Self::Adverse(m, t, s) => Some(adverse_price_gap(*m, *t, *s)?),
            Self::Round(v, s, d) => Some(v.round(*s, *d)?),
            Self::Vwap(levels, q) => vwap(levels, *q)?,
            Self::Executable(book, side, quantity) => book.executable(*side, *quantity)?,
            Self::Marginal(book, side, quantity) => book.marginal(*side, *quantity)?,
            Self::Fresh(book, now, healthy, policy) => {
                return Ok(Output::Boolean(
                    book.freshness(*now, *healthy, *policy) == Freshness::Fresh,
                ))
            }
        };
        Ok(Output::Number(value))
    }
}

fn evaluate(operation: &Result<Operation>) -> Result<Output> {
    match operation {
        Ok(op) => op.evaluate(),
        Err(e) => Err(*e),
    }
}

fn output(value: Result<Output>) -> Value {
    match value {
        Ok(Output::Number(n)) => json!({"value": n.map(|v| v.normalize().to_string())}),
        Ok(Output::Boolean(b)) => json!({"value": b}),
        Err(error) => json!({"error": error.to_string()}),
    }
}

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let arguments: Vec<_> = std::env::args().skip(1).collect();
    if arguments.iter().any(|a| a != "--benchmark") {
        return Err("usage: core-replay [--benchmark] < cases.json".into());
    }
    let mut input = String::new();
    io::stdin().read_to_string(&mut input)?;
    let inputs: Vec<Input> = serde_json::from_str(&input)?;
    if inputs.is_empty() {
        return Err("at least one case required".into());
    }
    let operations: Vec<_> = inputs.into_iter().map(Input::prepare).collect();
    let results: Vec<_> = operations.iter().map(|op| output(evaluate(op))).collect();
    let benchmark = if arguments.iter().any(|a| a == "--benchmark") {
        // Parse, quote validation, JSON output and process startup are excluded in both languages.
        // These are batch means, NOT individual event p95/p99 latency.
        let repeats = 100;
        for _ in 0..repeats {
            for op in &operations {
                let _ = black_box(evaluate(black_box(op)));
            }
        }
        let mut samples = Vec::new();
        for _ in 0..21 {
            let start = Instant::now();
            for _ in 0..repeats {
                for op in &operations {
                    let _ = black_box(evaluate(black_box(op)));
                }
            }
            samples.push(start.elapsed().as_nanos() as f64 / (repeats * operations.len()) as f64);
        }
        samples.sort_by(f64::total_cmp);
        json!({"rounds": 21, "repeats": repeats, "cases": operations.len(),
            "metric": "batch_mean_ns_per_case", "samples": samples,
            "p50": samples[10], "p95": samples[19], "profile": if cfg!(debug_assertions) {"debug"} else {"release"}})
    } else {
        Value::Null
    };
    println!("{}", json!({"results": results, "benchmark": benchmark}));
    Ok(())
}
