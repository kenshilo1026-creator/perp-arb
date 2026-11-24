use crate::{
    auth::canonical,
    client::Client,
    events::{self, Event},
    Error, Result,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{protocol::WebSocketConfig, Message},
    MaybeTlsStream, WebSocketStream,
};

/// One authenticated socket. Disconnection is explicit; callers must reconcile
/// tracked orders after a new subscription. No silent REST-only readiness.
pub struct Session {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    client: Client,
    subscription: u64,
}
impl Session {
    pub async fn connect(client: Client) -> Result<Self> {
        tokio::time::timeout(Duration::from_secs(10),async {
            let config=WebSocketConfig::default().max_frame_size(Some(1_048_576)).max_message_size(Some(1_048_576));
            let (mut ws,_)=connect_async_with_config(&client.0.ws_url,Some(config),true).await.map_err(|_|Error::Transport)?;
            let timestamp=client.timestamp()?;
            let fields=BTreeMap::from([("apiKey".into(),client.0.credentials.key().into()),("recvWindow".into(),"5000".into()),("timestamp".into(),timestamp.to_string())]);
            let signature=client.0.credentials.sign(&canonical(&fields)?);
            let message=json!({"id":"spot-orders","method":"userDataStream.subscribe.signature","params":{
                "apiKey":client.0.credentials.key(),"recvWindow":5000,"timestamp":timestamp,"signature":signature}});
            ws.send(Message::Text(message.to_string().into())).await.map_err(|_|Error::Transport)?;
            loop {
                match ws.next().await.ok_or(Error::StreamEnded)?.map_err(|_|Error::Transport)? {
                    Message::Ping(p)=>ws.send(Message::Pong(p)).await.map_err(|_|Error::Transport)?,
                    Message::Text(t)=>{
                        let v:Value=serde_json::from_str(&t).map_err(|_|Error::Protocol)?;
                        if v["id"]!="spot-orders" {return Err(Error::Protocol);}
                        if v["status"].as_u64()!=Some(200) {
                            return Err(Error::Api{status:v["status"].as_u64().and_then(|x|u16::try_from(x).ok()).unwrap_or(0),code:v["error"]["code"].as_i64()});
                        }
                        let subscription=v["result"]["subscriptionId"].as_u64().ok_or(Error::Protocol)?;
                        return Ok(Self{ws,client,subscription});
                    },
                    Message::Pong(_)=>(),_=>return Err(Error::StreamEnded),
                }
            }
        }).await.map_err(|_|Error::Timeout)?
    }
    pub async fn next(&mut self) -> Result<Event> {
        loop {
            let msg = tokio::time::timeout(Duration::from_secs(60), self.ws.next())
                .await
                .map_err(|_| Error::Timeout)?
                .ok_or(Error::StreamEnded)?
                .map_err(|_| Error::Transport)?;
            let received_at = Instant::now();
            match msg {
                Message::Text(t) => {
                    let v: Value = serde_json::from_str(&t).map_err(|_| Error::Protocol)?;
                    return events::stream(
                        &self.client.scope(),
                        self.subscription,
                        &v,
                        received_at,
                    );
                }
                Message::Ping(p) => self
                    .ws
                    .send(Message::Pong(p))
                    .await
                    .map_err(|_| Error::Transport)?,
                Message::Pong(_) => (),
                _ => return Err(Error::StreamEnded),
            }
        }
    }
    pub async fn close(mut self) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(2), self.ws.close(None))
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| Error::Transport)
    }
}
