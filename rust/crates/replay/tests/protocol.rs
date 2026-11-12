use serde_json::{json, Value};
use std::{
    io::Write,
    process::{Command, Stdio},
};

fn run(input: Value) -> std::process::Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_core-replay"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.to_string().as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}

#[test]
fn explicit_empty_and_unsized_depth_produce_different_results() {
    let mut quote = json!({"bid":"99", "ask":"100", "received_ms":10000, "source_ms":10000, "bids":[], "asks":[]});
    let empty = json!({"op":"executable", "quote":quote, "side":"BUY", "quantity":"1"});
    quote["bids"] = Value::Null;
    quote["asks"] = Value::Null;
    let unpublished = json!({"op":"executable", "quote":quote, "side":"BUY", "quantity":"1"});
    let output = run(json!([empty, unpublished]));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["results"], json!([{"value":null}, {"value":"100"}]));
}

#[test]
fn omitted_depth_and_bad_side_are_protocol_errors() {
    let omitted = json!({"op":"executable", "quote":{"bid":"99", "ask":"100", "received_ms":10000}, "side":"BUY", "quantity":"1"});
    assert!(!run(json!([omitted])).status.success());
    assert!(
        !run(json!([{"op":"adverse", "maker":"100", "taker":"101", "side":"INVALID"}]))
            .status
            .success()
    );
    assert!(!run(json!([])).status.success());
}

#[test]
fn arithmetic_failure_is_explicit_and_does_not_lose_following_cases() {
    let output = run(json!([
        {"op":"spread", "short":"100", "long":"0"},
        {"op":"spread", "short":"NaN", "long":"100"},
        {"op":"spread", "short":"2010", "long":"2000"}
    ]));
    assert!(output.status.success());
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        result["results"],
        json!([{"error":"NonPositive"}, {"error":"InvalidDecimal"}, {"value":"50"}])
    );
}
