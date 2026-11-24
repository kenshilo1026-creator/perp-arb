//! Private transport fencing for account reports. WS portfolio updates are NOT
//! complete inventory snapshots. Only REST inventory queries can produce one.
use crate::{
    wire::{Channel, Event, Observation, Position},
    Result,
};
use perp_arb_private_events::{
    actor::{self, Coordinator, TransportLease},
    Scope, Session,
};
pub struct Feed {
    coordinator: Coordinator,
    _lease: TransportLease,
    worker: tokio::task::JoinHandle<()>,
    session: Option<Session>,
    generation: u64,
    portfolio: Option<(Vec<Position>, Option<String>)>,
    last_capture: Option<i64>,
    awaiting_valid: bool,
}
impl Feed {
    pub fn start(account: &str) -> Result<Self> {
        let (coordinator, worker) = actor::channel(
            Scope {
                venue: "variational".into(),
                account: crate::native::account(account)?,
            },
            1024,
            1024,
        )
        .map_err(|e| e.to_string())?;
        let lease = coordinator.acquire_transport().map_err(|e| e.to_string())?;
        Ok(Self {
            coordinator,
            _lease: lease,
            worker: worker.spawn(),
            session: None,
            generation: 0,
            portfolio: None,
            last_capture: None,
            awaiting_valid: false,
        })
    }
    pub fn coordinator(&self) -> Coordinator {
        self.coordinator.clone()
    }
    pub async fn ingest(&mut self, event: &Event) -> Result<()> {
        if event.channel != Channel::Private {
            return Ok(());
        }
        if event.generation < self.generation {
            return Err("old account connection generation".into());
        }
        if matches!(event.observation, Observation::Connected) {
            if event.generation <= self.generation {
                return Err("duplicate account connection".into());
            }
            self.generation = event.generation;
            self.portfolio = None;
            self.last_capture = None;
            self.session = Some(
                self.coordinator
                    .connected()
                    .await
                    .map_err(|e| e.to_string())?,
            );
            self.awaiting_valid = false;
            return Ok(());
        }
        if event.generation != self.generation || self.session.is_none() {
            return Err("account event outside connected generation".into());
        }
        if matches!(
            event.observation,
            Observation::Gap { .. } | Observation::Disconnected
        ) {
            self.coordinator
                .transport_lost()
                .await
                .map_err(|e| e.to_string())?;
            self.portfolio = None;
            self.last_capture = None;
            if matches!(event.observation, Observation::Disconnected) {
                self.session = None;
            } else {
                self.session = Some(
                    self.coordinator
                        .connecting()
                        .await
                        .map_err(|e| e.to_string())?,
                );
                self.awaiting_valid = true;
            }
            return Ok(());
        }
        let captured = event.captured_ms.ok_or("account capture time missing")?;
        if self.last_capture.is_some_and(|t| captured < t) {
            self.coordinator
                .transport_lost()
                .await
                .map_err(|e| e.to_string())?;
            return Err("account capture moved backwards".into());
        }
        self.last_capture = Some(captured);
        if self.awaiting_valid {
            self.coordinator
                .activate(self.session.clone().ok_or("session missing")?)
                .await
                .map_err(|e| e.to_string())?;
            self.awaiting_valid = false;
        }
        let changed = match &event.observation {
            Observation::Heartbeat => false,
            Observation::Portfolio {
                positions,
                wallet_balance,
            } => {
                let next = (positions.clone(), wallet_balance.clone());
                let changed = self.portfolio.as_ref() != Some(&next);
                self.portfolio = Some(next);
                changed
            }
            // Trades, clearing, transfers, funding and unknown schemas invalidate
            // proof, even when they don't change a recognized position field.
            _ => true,
        };
        if changed {
            self.coordinator
                .account_changed(self.session.clone().ok_or("session missing")?)
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }
    pub async fn shutdown(self) -> Result<()> {
        self.coordinator
            .shutdown()
            .await
            .map_err(|e| e.to_string())?;
        self.worker
            .await
            .map_err(|_| "account worker stopped".into())
    }
}
