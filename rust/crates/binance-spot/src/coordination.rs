//! Connect verified Binance evidence to the shared cumulative order actor.
//! Gross execution is separate from Fees::reconcile's net inventory evidence.
//! The socket owner must call transport_lost/disconnected on every disconnect
//! and obtain a new actor Session only after a new authenticated WS ACK.
use crate::{client::Client, events::OrderEvidence, Error, Result};
use perp_arb_private_events::{
    actor::Coordinator,
    lookup::{Request, Selector},
    Evidence, OrderKey, OrderSnapshot, Session,
};

pub async fn observe_ws(
    coordinator: &Coordinator,
    session: Session,
    request: &Request,
    e: OrderEvidence,
) -> Result<OrderSnapshot> {
    if coordinator.scope() != &request.scope {
        return Err(Error::Identity);
    }
    request.verify(&e.found).map_err(|_| Error::Identity)?;
    coordinator
        .observe(e.found.order, Evidence::Ws(session), e.received_at)
        .await
        .map_err(|_| Error::Protocol)
}
/// Run independently of the WS receive task. Capture the actor query ticket
/// before network IO, so a delayed REST reply cannot erase newer WS fills.
pub async fn reconcile(
    client: &Client,
    coordinator: &Coordinator,
    request: &Request,
) -> Result<Option<OrderSnapshot>> {
    if coordinator.scope() != &client.scope() || coordinator.scope() != &request.scope {
        return Err(Error::Identity);
    }
    let Selector::OrderId(id) = &request.selector else {
        return Err(Error::Configuration);
    };
    let key = OrderKey {
        symbol: request.symbol.clone(),
        order_id: id.clone(),
    };
    let ticket = coordinator
        .begin_query(key.clone())
        .await
        .map_err(|_| Error::Protocol)?;
    match client.query(request).await {
        Ok(Some(e)) => coordinator
            .observe(e.found.order, Evidence::Rest(ticket), e.received_at)
            .await
            .map(Some)
            .map_err(|_| Error::Protocol),
        outcome => {
            coordinator
                .require_reconciliation(key)
                .await
                .map_err(|_| Error::Protocol)?;
            outcome.map(|_| None)
        }
    }
}
