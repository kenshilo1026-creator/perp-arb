use crate::Result;
use perp_arb_aster_private::{
    auth,
    client::{HttpPolicy, PrivateHttp},
};
use perp_arb_hyperliquid_private::{
    client::{Client, TradingClient},
    signing,
};
use perp_arb_private_events::{
    actor::Coordinator,
    lookup::{Outcome, Request},
    orders::{Accepted, Network, Order},
};
use zeroize::Zeroizing;
pub trait Venue {
    fn submit(&self, order: &Order) -> impl std::future::Future<Output = Result<Accepted>> + Send;
    fn cancel(&self, order: &Order) -> impl std::future::Future<Output = Result<()>> + Send;
    fn lookup(
        &self,
        request: &Request,
    ) -> impl std::future::Future<Output = Result<Outcome>> + Send;
}
pub fn hl_client(account: &str, network: Network) -> Result<Client> {
    match network {
        Network::Mainnet => Client::new(account),
        Network::Testnet => Client::testnet(account),
    }
    .map_err(|e| e.to_string())
}
pub enum LiveVenue {
    Lighter(perp_arb_lighter_private::Client),
    Aster(PrivateHttp),
    Hyperliquid { read: Client, trade: TradingClient },
}
pub enum Monitor {
    Lighter(perp_arb_lighter_private::monitor::Monitor),
    Aster(perp_arb_aster_private::monitor::Monitor),
    Hyperliquid(perp_arb_hyperliquid_private::monitor::Monitor),
}
impl Monitor {
    pub async fn shutdown(self) -> Result<()> {
        match self {
            Self::Lighter(m) => m.shutdown().await,
            Self::Aster(m) => m.shutdown().await.map_err(|e| e.to_string()),
            Self::Hyperliquid(m) => m.shutdown().await.map_err(|e| e.to_string()),
        }
    }
}
impl LiveVenue {
    pub async fn check_position(
        &self,
        c: &Coordinator,
        symbol: &str,
    ) -> Result<perp_arb_private_events::account::Report> {
        match self {
            Self::Lighter(client) => client.check_position(c, symbol).await,
            Self::Aster(client) => client
                .check_position(c, symbol)
                .await
                .map_err(|e| e.to_string()),
            Self::Hyperliquid { read, .. } => read
                .check_position(c, symbol)
                .await
                .map_err(|e| e.to_string()),
        }
    }
    pub async fn configure_isolated(&self, order: &Order, leverage: u32) -> Result<()> {
        match self {
            Self::Lighter(c) => c.configure_isolated(order, leverage).await,
            Self::Aster(c) => c
                .configure_isolated(order, leverage)
                .await
                .map_err(|e| e.to_string()),
            Self::Hyperliquid { trade, .. } => trade
                .configure_isolated(order, leverage)
                .await
                .map_err(|e| e.to_string()),
        }
    }
    pub async fn audit(
        &self,
        c: &Coordinator,
        expected: Option<perp_arb_private_events::account::Inventory>,
    ) -> Result<perp_arb_private_events::account::Report> {
        match self {
            Self::Lighter(client) => client.audit(c, expected).await,
            Self::Aster(client) => client
                .audit_account(c, expected)
                .await
                .map_err(|e| e.to_string()),
            Self::Hyperliquid { read, .. } => read
                .audit_account(c, &[String::new()], false, expected)
                .await
                .map_err(|e| e.to_string()),
        }
    }
    pub async fn connect(order: &Order) -> Result<Self> {
        order.validate().map_err(|e| e.to_string())?;
        match order.venue.as_str() {
            "lighter" => Ok(Self::Lighter(
                perp_arb_lighter_private::Client::connect(order).await?,
            )),
            "aster" => {
                let key = Zeroizing::new(
                    std::env::var("ASTER_API_WALLET_PRIVATE_KEY")
                        .map_err(|_| "missing ASTER_API_WALLET_PRIVATE_KEY")?,
                );
                let credentials =
                    auth::Credentials::new(&key, &order.account).map_err(|e| e.to_string())?;
                Ok(Self::Aster(
                    PrivateHttp::new(credentials, HttpPolicy::default())
                        .map_err(|e| e.to_string())?,
                ))
            }
            "hyperliquid" => {
                let key = Zeroizing::new(
                    std::env::var("HYPERLIQUID_PRIVATE_KEY")
                        .map_err(|_| "missing HYPERLIQUID_PRIVATE_KEY")?,
                );
                let read = hl_client(&order.account, order.network)?;
                let credentials = signing::Credentials::new(&key).map_err(|e| e.to_string())?;
                let trade = read.trading(credentials).await.map_err(|e| e.to_string())?;
                Ok(Self::Hyperliquid { read, trade })
            }
            _ => Err("unsupported venue".into()),
        }
    }
    pub async fn monitor(&self, c: Coordinator) -> Result<Monitor> {
        match self {
            Self::Lighter(client) => perp_arb_lighter_private::monitor::start(client.clone(), c)
                .await
                .map(Monitor::Lighter),
            Self::Aster(client) => {
                perp_arb_aster_private::monitor::start(client.clone(), c, Default::default())
                    .await
                    .map(Monitor::Aster)
                    .map_err(|e| e.to_string())
            }
            Self::Hyperliquid { read, .. } => {
                perp_arb_hyperliquid_private::monitor::start(read.clone(), c, Default::default())
                    .await
                    .map(Monitor::Hyperliquid)
                    .map_err(|e| e.to_string())
            }
        }
    }
}
impl Venue for LiveVenue {
    async fn submit(&self, o: &Order) -> Result<Accepted> {
        match self {
            Self::Lighter(c) => c.submit(o).await,
            Self::Aster(c) => {
                if o.tif == perp_arb_private_events::orders::TimeInForce::Market {
                    c.submit_market(o).await.map_err(|e| e.to_string())
                } else {
                    c.submit_limit(o).await.map_err(|e| e.to_string())
                }
            }
            Self::Hyperliquid { trade, .. } => {
                trade.submit_limit(o).await.map_err(|e| e.to_string())
            }
        }
    }
    async fn cancel(&self, o: &Order) -> Result<()> {
        match self {
            Self::Lighter(c) => c.cancel(o).await,
            Self::Aster(c) => c.cancel_limit(o).await.map_err(|e| e.to_string()),
            Self::Hyperliquid { trade, .. } => {
                trade.cancel_limit(o).await.map_err(|e| e.to_string())
            }
        }
    }
    async fn lookup(&self, r: &Request) -> Result<Outcome> {
        match self {
            Self::Lighter(c) => c.lookup(r).await,
            Self::Aster(c) => c.lookup_submission(r).await.map_err(|e| e.to_string()),
            Self::Hyperliquid { read, .. } => {
                read.lookup_submission(r).await.map_err(|e| e.to_string())
            }
        }
    }
}
