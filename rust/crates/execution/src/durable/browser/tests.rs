use super::*;
use std::path::PathBuf;

fn trade(id: &str, quantity: &str) -> ConfirmedTrade {
    ConfirmedTrade {
        id: id.into(),
        quantity: quantity.into(),
        price: "100".into(),
    }
}
fn record_trades(l: &mut Ledger, q: &str, trades: Vec<ConfirmedTrade>) -> JResult<()> {
    let ticket = l.lookup_ticket()?;
    let mut s = snapshot(q, Status::PartiallyFilled, 100);
    s.requires_reconciliation = true;
    l.apply_verified_trades(ticket, &s, trades)
}
#[test]
fn trade_union_survives_restart_and_overlapping_or_truncated_pages() {
    let (path, mut l) = setup();
    record_trades(&mut l, "2", vec![trade("a", "2")]).unwrap();
    drop(l);
    let mut l = Ledger::open(&path.0).unwrap();
    record_trades(&mut l, "3", vec![trade("b", "1"), trade("a", "2")]).unwrap();
    record_trades(&mut l, "1", vec![trade("b", "1")]).unwrap();
    record_trades(&mut l, "2.0", vec![trade("a", "2.0"), trade("a", "2")]).unwrap();
    assert_eq!(l.view().cumulative, "3");
    assert_eq!(l.view().confirmed_trade_count, 2);
    assert!(l.view().reconciliation_required);
    assert!(l.dispatch().is_err());
}
#[test]
fn legacy_anonymous_cumulative_is_floor_not_added_to_trade_union() {
    let (_path, mut l) = setup();
    l.observe_verified(&snapshot("3", Status::PartiallyFilled, 90))
        .unwrap();
    record_trades(&mut l, "2", vec![trade("a", "2")]).unwrap();
    assert_eq!(l.view().cumulative, "3");
    record_trades(&mut l, "2", vec![trade("b", "2")]).unwrap();
    assert_eq!(l.view().cumulative, "4"); // never 3 + 2 + 2
}
#[test]
fn conflicting_trade_identity_is_sticky_across_restart() {
    for change_price in [false, true] {
        let (path, mut l) = setup();
        record_trades(&mut l, "2", vec![trade("a", "2")]).unwrap();
        let mut bad = trade("a", if change_price { "2" } else { "3" });
        if change_price {
            bad.price = "99".into();
        }
        let q = bad.quantity.clone();
        assert!(record_trades(&mut l, &q, vec![bad]).is_err());
        assert_eq!(l.view().cumulative, "2");
        drop(l);
        let mut l = Ledger::open(&path.0).unwrap();
        assert!(l.view().trade_conflict);
        assert!(record_trades(&mut l, "2", vec![trade("a", "2")]).is_err());
        let ticket = l.lookup_ticket().unwrap();
        assert!(l
            .apply_verified_lookup(ticket, &snapshot("10", Status::Filled, 200))
            .is_err());
    }
}
#[test]
fn union_overfill_or_inexact_sum_cannot_change_exposure() {
    let (_path, mut l) = setup();
    record_trades(&mut l, "6", vec![trade("a", "6")]).unwrap();
    assert!(record_trades(&mut l, "6", vec![trade("b", "6")]).is_err());
    assert_eq!(l.view().cumulative, "6");
    assert!(l.view().trade_conflict);
    let (_path2, mut l2) = setup();
    assert!(record_trades(
        &mut l2,
        "1",
        vec![
            trade("tiny", "0.0000000000000000000000000001"),
            trade("one", "1")
        ]
    )
    .is_err());
    assert_eq!(l2.view().cumulative, "0");
}
#[test]
fn trade_write_failure_keeps_known_exposure_but_no_permit() {
    let (path, mut l) = setup();
    l.journal.fail = Some(false);
    assert!(record_trades(&mut l, "2", vec![trade("a", "2")]).is_err());
    assert_eq!(l.view().cumulative, "2");
    assert_eq!(l.view().confirmed_trade_count, 1);
    assert!(l.view().poisoned);
    assert!(l.cancel_started().is_err());
    drop(l);
    let l = Ledger::open(&path.0).unwrap();
    assert!(l.view().reconciliation_required);
}
#[test]
fn complete_trade_query_confirms_terminal_but_gap_invalidates_ticket() {
    let (_path, mut l) = setup();
    let t = l.lookup_ticket().unwrap();
    l.unresolved().unwrap();
    l.apply_verified_trades(
        t,
        &snapshot("10", Status::Filled, 100),
        vec![trade("a", "10")],
    )
    .unwrap();
    assert!(!l.view().terminal_confirmed);
    let t = l.lookup_ticket().unwrap();
    l.apply_verified_trades(
        t,
        &snapshot("10", Status::Filled, 101),
        vec![trade("a", "10")],
    )
    .unwrap();
    assert!(l.view().terminal_confirmed);
    assert_eq!(l.view().confirmed_trade_count, 1);
}
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!(
            "browser-maker-{:x}.wal",
            u128::from_le_bytes(random().unwrap())
        )))
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
fn command() -> Command {
    Command {
        request_id: "rust-request-1".into(),
        account: "fixture-account".into(),
        symbol: "ETH".into(),
        side: "BUY".into(),
        quantity: "10".into(),
        limit_price: "100".into(),
        reduce_only: false,
        max_notional: "1000".into(),
    }
}
fn identity(c: &Command) -> Identity {
    Identity {
        request_id: c.request_id.clone(),
        account: c.account.clone(),
        symbol: c.symbol.clone(),
        side: c.side.clone(),
        quantity: c.quantity.clone(),
        reduce_only: c.reduce_only,
        order_id: "venue-order-1".into(),
    }
}
fn setup() -> (Temp, Ledger) {
    let path = Temp::new();
    let mut ledger = Ledger::create(&path.0, command()).unwrap();
    ledger.dispatch().unwrap();
    ledger.bind_verified(identity(&command())).unwrap();
    (path, ledger)
}
fn snapshot(cumulative: &str, status: Status, ms: u64) -> OrderSnapshot {
    let now = Instant::now();
    OrderSnapshot {
        scope: command().scope(),
        key: OrderKey {
            symbol: "ETH".into(),
            order_id: "venue-order-1".into(),
        },
        original: qty("10").unwrap(),
        cumulative: qty(cumulative).unwrap(),
        status,
        event_ms: Some(ms),
        revision: 1,
        requires_reconciliation: false,
        observed_at: now,
        applied_at: now,
    }
}
fn confirm(ledger: &mut Ledger, q: &str, status: Status, ms: u64) {
    let ticket = ledger.lookup_ticket().unwrap();
    ledger
        .apply_verified_lookup(ticket, &snapshot(q, status, ms))
        .unwrap();
}

#[test]
fn ack_and_unknown_never_authorize_resubmit_even_after_restart() {
    let path = Temp::new();
    let mut l = Ledger::create(&path.0, command()).unwrap();
    assert!(l.submit_ack("rust-request-1").is_err());
    let permit = l.dispatch().unwrap();
    assert_eq!(permit.command(), &command());
    l.submit_ack("rust-request-1").unwrap();
    l.unresolved().unwrap();
    assert_eq!(l.view().cumulative, "0");
    assert!(l.view().order_id.is_none());
    assert!(l.lookup_ticket().is_err());
    assert!(l.dispatch().is_err());
    drop(l);
    let mut l = Ledger::open(&path.0).unwrap();
    assert!(l.dispatch().is_err());
    assert!(l.view().dispatched);
    assert!(l.view().reconciliation_required);
}
#[test]
fn close_intent_and_quantity_are_preserved_on_disk() {
    let path = Temp::new();
    let mut c = command();
    c.reduce_only = true;
    c.side = "SELL".into();
    let mut l = Ledger::create(&path.0, c.clone()).unwrap();
    assert!(l.dispatch().unwrap().command().reduce_only);
    l.bind_verified(identity(&c)).unwrap();
    drop(l);
    let l = Ledger::open(&path.0).unwrap();
    assert_eq!(l.view().command, c);
}
#[test]
fn exact_binding_rejects_every_identity_mismatch() {
    for field in 0..7 {
        let path = Temp::new();
        let mut l = Ledger::create(&path.0, command()).unwrap();
        l.dispatch().unwrap();
        let mut bad = identity(&command());
        match field {
            0 => bad.account = "other".into(),
            1 => bad.symbol = "BTC".into(),
            2 => bad.side = "SELL".into(),
            3 => bad.quantity = "11".into(),
            4 => bad.reduce_only = true,
            5 => bad.request_id = "other".into(),
            _ => bad.order_id = "".into(),
        }
        assert!(l.bind_verified(bad).is_err());
        assert!(l.view().order_id.is_none());
    }
}
#[test]
fn fills_before_ui_ack_are_retained_only_after_exact_binding() {
    let path = Temp::new();
    let mut l = Ledger::create(&path.0, command()).unwrap();
    l.dispatch().unwrap();
    assert!(l
        .observe_verified(&snapshot("2", Status::PartiallyFilled, 10))
        .is_err());
    l.bind_verified(identity(&command())).unwrap();
    assert_eq!(
        l.observe_verified(&snapshot("2", Status::PartiallyFilled, 10))
            .unwrap(),
        qty("2").unwrap()
    );
    l.submit_ack("rust-request-1").unwrap();
    assert_eq!(l.view().cumulative, "2");
}
#[test]
fn duplicates_and_old_cumulative_cannot_erase_or_double_count_fills() {
    let (_path, mut l) = setup();
    assert_eq!(
        l.observe_verified(&snapshot("2", Status::PartiallyFilled, 20))
            .unwrap(),
        qty("2").unwrap()
    );
    assert_eq!(
        l.observe_verified(&snapshot("2", Status::PartiallyFilled, 20))
            .unwrap(),
        Decimal::ZERO
    );
    assert_eq!(
        l.observe_verified(&snapshot("1", Status::PartiallyFilled, 10))
            .unwrap(),
        Decimal::ZERO
    );
    assert_eq!(l.view().cumulative, "2");
    assert!(l.view().reconciliation_required);
}
#[test]
fn cancel_ack_and_ui_absence_do_not_prove_terminal_and_late_fill_survives() {
    let (_path, mut l) = setup();
    l.observe_verified(&snapshot("2", Status::PartiallyFilled, 10))
        .unwrap();
    assert_eq!(l.cancel_started().unwrap().order_id(), "venue-order-1");
    l.unresolved().unwrap();
    assert!(!l.view().terminal_confirmed);
    assert!(l.view().cancel_pending);
    assert!(l.cancel_started().is_err());
    assert_eq!(
        l.observe_verified(&snapshot("3", Status::Canceled, 11))
            .unwrap(),
        Decimal::ONE
    );
    assert!(l.maker_snapshot().unwrap().requires_reconciliation);
    confirm(&mut l, "3", Status::Canceled, 12);
    assert!(l.view().terminal_confirmed);
    assert!(!l.view().cancel_pending);
}
#[test]
fn terminal_ws_without_new_fill_still_needs_exact_lookup() {
    let (_path, mut l) = setup();
    confirm(&mut l, "2", Status::PartiallyFilled, 10);
    l.observe_verified(&snapshot("2", Status::Canceled, 11))
        .unwrap();
    assert!(!l.view().terminal_confirmed);
    assert!(l.maker_snapshot().unwrap().requires_reconciliation);
}
#[test]
fn racing_lookup_keeps_larger_cumulative_but_cannot_release_remaining_maker() {
    let (_path, mut l) = setup();
    let t = l.lookup_ticket().unwrap();
    l.observe_verified(&snapshot("2", Status::PartiallyFilled, 10))
        .unwrap();
    l.apply_verified_lookup(t, &snapshot("3", Status::Canceled, 11))
        .unwrap();
    assert_eq!(l.view().cumulative, "3");
    assert!(!l.view().terminal_confirmed);
    assert!(l.reconciliation_snapshot().is_err());
    confirm(&mut l, "3", Status::Canceled, 12);
    assert!(l.view().terminal_confirmed);
}
#[test]
fn delayed_lookup_and_disconnect_fence_cannot_confirm_terminal() {
    for elapsed in [true, false] {
        let (_path, mut l) = setup();
        let mut t = l.lookup_ticket().unwrap();
        if elapsed {
            t.started -= Duration::from_secs(6);
        } else {
            l.unresolved().unwrap();
        }
        l.apply_verified_lookup(t, &snapshot("10", Status::Filled, 10))
            .unwrap();
        assert_eq!(l.view().cumulative, "10");
        assert!(!l.view().terminal_confirmed);
    }
}
#[test]
fn restart_preserves_fills_but_requires_fresh_lookup_not_old_ticket() {
    let (path, mut l) = setup();
    confirm(&mut l, "2", Status::Canceled, 10);
    let t = l.lookup_ticket().unwrap();
    drop(l);
    let mut l = Ledger::open(&path.0).unwrap();
    assert_eq!(l.view().cumulative, "2");
    assert!(!l.view().terminal_confirmed);
    assert!(l.reconciliation_snapshot().is_err());
    assert!(l
        .apply_verified_lookup(t, &snapshot("2", Status::Canceled, 11))
        .is_err());
    confirm(&mut l, "2", Status::Canceled, 12);
    assert!(l.reconciliation_snapshot().is_ok());
}
#[test]
fn fill_after_confirmed_cancel_is_not_hidden() {
    let (_path, mut l) = setup();
    confirm(&mut l, "2", Status::Canceled, 10);
    assert_eq!(
        l.observe_verified(&snapshot("3", Status::Canceled, 11))
            .unwrap(),
        Decimal::ONE
    );
    assert!(!l.view().terminal_confirmed);
    assert_eq!(l.maker_snapshot().unwrap().cumulative, qty("3").unwrap());
}
#[test]
fn source_watermark_never_rolls_back_on_larger_old_fill() {
    let (_path, mut l) = setup();
    l.observe_verified(&snapshot("2", Status::PartiallyFilled, 100))
        .unwrap();
    l.observe_verified(&snapshot("3", Status::PartiallyFilled, 50))
        .unwrap();
    confirm(&mut l, "3", Status::Canceled, 60);
    assert!(!l.view().terminal_confirmed);
    confirm(&mut l, "3", Status::Canceled, 101);
    assert!(l.view().terminal_confirmed);
}
#[test]
fn wrong_order_and_overfill_never_modify_known_quantity() {
    let (_path, mut l) = setup();
    let mut s = snapshot("2", Status::PartiallyFilled, 10);
    s.key.order_id = "wrong".into();
    assert!(l.observe_verified(&s).is_err());
    assert!(l
        .observe_verified(&snapshot("11", Status::Filled, 10))
        .is_err());
    assert_eq!(l.view().cumulative, "0");
}
#[test]
fn failed_disk_sync_never_returns_dispatch_or_cancel_permission() {
    let path = Temp::new();
    let mut l = Ledger::create(&path.0, command()).unwrap();
    l.journal.fail = Some(false);
    assert!(l.dispatch().is_err());
    assert!(l.view().poisoned);
    assert!(l.dispatch().is_err());
    drop(l);
    let mut l = Ledger::open(&path.0).unwrap();
    assert!(l.view().dispatched);
    assert!(l.dispatch().is_err());
    let (_p, mut other) = setup();
    other.journal.fail = Some(false);
    assert!(other.cancel_started().is_err());
    assert!(other.view().poisoned);
}
#[test]
fn file_lock_truncation_and_exact_notional_are_enforced() {
    let path = Temp::new();
    let mut bad = command();
    bad.max_notional = "999.9999999999999999999999999".into();
    assert!(Ledger::create(&path.0, bad).is_err());
    assert!(!path.0.exists());
    let l = Ledger::create(&path.0, command()).unwrap();
    assert!(Ledger::open(&path.0).is_err());
    drop(l);
    let mut bytes = std::fs::read(&path.0).unwrap();
    bytes.pop();
    std::fs::write(&path.0, bytes).unwrap();
    assert!(matches!(Ledger::open(&path.0), Err(JournalError::Corrupt)));
}

#[test]
fn failed_fill_write_retains_visible_exposure_and_never_grants_permission() {
    let (path, mut l) = setup();
    l.journal.fail = Some(false);
    assert!(l
        .observe_verified(&snapshot("2", Status::PartiallyFilled, 10))
        .is_err());
    assert_eq!(l.view().cumulative, "2");
    assert!(l.view().poisoned);
    assert!(l.dispatch().is_err());
    assert!(l.cancel_started().is_err());
    drop(l);
    let l = Ledger::open(&path.0).unwrap();
    assert_eq!(l.view().cumulative, "2");
    assert!(l.view().reconciliation_required);
}

#[test]
fn rounded_delta_is_rejected_but_confirmed_cumulative_remains_visible() {
    let path = Temp::new();
    let mut c = command();
    c.quantity = "10000000000000000000000000000".into();
    c.limit_price = "1".into();
    c.max_notional = c.quantity.clone();
    let mut l = Ledger::create(&path.0, c.clone()).unwrap();
    l.dispatch().unwrap();
    l.bind_verified(identity(&c)).unwrap();
    let mut small = snapshot("0.0000000000000000000000000001", Status::PartiallyFilled, 1);
    small.original = qty(&c.quantity).unwrap();
    l.observe_verified(&small).unwrap();
    let mut large = small.clone();
    large.cumulative = small.original;
    large.status = Status::Filled;
    large.event_ms = Some(2);
    assert!(l.observe_verified(&large).is_err());
    assert_eq!(qty(&l.view().cumulative).unwrap(), large.cumulative);
}
