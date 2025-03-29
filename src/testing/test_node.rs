use std::{sync::Arc, time::Duration};

use crate::{
    ConnectionManager,
    basic::{BasicConnectionManager, ConnectionHandler},
    event::{Event, EventMappingShared, EventType},
    testing::discover,
};
use anyhow::Result;
use futures::{
    FutureExt,
    future::{self, BoxFuture},
};
use iroh::{
    Endpoint, NodeId,
    endpoint::{ConnectOptions, Connection, TransportConfig, VarInt},
};
use n0_future::future::Boxed;

pub const ALPN_ECHO: &[u8] = b"test/echo";

#[derive(Clone, Debug, derive_more::Deref)]
pub struct TestNode {
    #[deref]
    manager: Arc<BasicConnectionManager>,
}

impl TestNode {
    pub async fn new(
        handler: impl ConnectionHandler<EchoConnection>,
        alpns: impl IntoIterator<Item = Vec<u8>>,
        mapping: EventMappingShared,
    ) -> Result<Self> {
        let alpns = alpns.into_iter().collect::<Vec<_>>();
        let endpoint = Endpoint::builder().alpns(alpns.clone()).bind().await?;
        let manager = BasicConnectionManager::spawn(endpoint.clone(), mapping);
        manager.register_handler(alpns, handler).await?;
        Ok(Self { manager })
    }

    pub async fn cluster<const N: usize>(
        handler: impl ConnectionHandler<EchoConnection> + Clone,
        alpns: impl IntoIterator<Item = Vec<u8>>,
        mapping: EventMappingShared,
    ) -> Result<[Self; N]> {
        let alpns = alpns.into_iter().collect::<Vec<_>>();
        let nodes = future::join_all(
            std::iter::repeat_with(|| {
                TestNode::new(handler.clone(), alpns.clone(), mapping.clone())
            })
            .take(N),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;

        let endpoints = nodes
            .iter()
            .map(|n| n.manager.endpoint())
            .collect::<Vec<_>>();

        discover(endpoints).await?;

        Ok(nodes.try_into().unwrap())
    }

    #[tracing::instrument(skip_all, fields(node = self.endpoint().node_id().fmt_short(), remote = n.endpoint().node_id().fmt_short()))]
    pub async fn connect(&self, n: &Self, alpn: &[u8]) -> Result<Connection> {
        let conn = self
            .get_or_open_connection(n.manager.endpoint().node_id(), alpn)
            .await?;
        Ok(conn)
    }

    /// Make a single RPC to the given node
    pub async fn rpc(&self, n: &Self, msg: &[u8]) -> Result<Vec<u8>> {
        tracing::info!(
            "[caller] BEGIN {} -> {}",
            self.endpoint().node_id().fmt_short(),
            n.endpoint().node_id().fmt_short()
        );
        let conn = self.connect(n, ALPN_ECHO).await?;
        let (mut send, mut recv) = conn.open_bi().await?;
        self.manager
            .emit_event(
                EventType::OpenStream {
                    to: n.endpoint().node_id(),
                    conn: conn.stable_id(),
                },
                None,
            )
            .await;

        tracing::debug!(send = ?send.id(), recv = ?recv.id(), "[caller]");
        send.write_all(msg).await?;
        tracing::debug!("[caller] wrote data: {}", std::str::from_utf8(msg)?);
        send.finish()?;
        tracing::debug!("[caller] finished sending");
        let response = recv.read_to_end(10_000).await?;
        tracing::info!("[caller] DONE");
        assert_eq!(msg, &response);

        self.manager
            .emit_event(
                EventType::EndStream {
                    to: n.endpoint().node_id(),
                    conn: conn.stable_id(),
                },
                None,
            )
            .await;

        Ok(response)
    }

    /// Given a list of nodes, have each node simultaneously make an RPC to the
    /// next node in the list in a circular manner.
    pub async fn rpc_cycle(ns: impl IntoIterator<Item = &Self>, msg: &[u8]) -> Result<()> {
        let ns = ns.into_iter().collect::<Vec<_>>();
        let num = ns.len();
        let mut calls = vec![];
        for i in 0..num {
            let j = (i + 1) % num;
            let n = ns[i].clone();
            let m = ns[j].clone();
            let epn = n.manager.endpoint().node_id().fmt_short();
            let epm = m.manager.endpoint().node_id().fmt_short();
            calls.push(async move {
                n.rpc(&m, msg).await.map_err(move |e| {
                    anyhow::anyhow!("rpc_cycle error: {e}, {i} ({epn}) -> {j} ({epm})")
                })
            });
        }

        let results = future::join_all(calls)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        assert!(
            results.iter().all(|r| r == msg),
            "all results must match the original message"
        );

        Ok(())
    }
}

/// Artificial "processing time" delay for the echo handler
const ECHO_DELAY: Duration = Duration::from_millis(100);

#[derive(Clone)]
pub struct EchoHandler(EventMappingShared);

#[derive(Clone, derive_more::Deref)]
pub struct EchoConnection {
    #[deref]
    pub conn: Connection,

    pub id: u64,
}

impl ConnectionHandler<EchoConnection> for EchoHandler {
    fn connect_options(&self) -> ConnectOptions {
        // 1 second idle timeout for these tests
        ConnectOptions::new().with_transport_config({
            let mut cfg = TransportConfig::default();
            cfg.max_idle_timeout(Some(VarInt::from_u32(1_000).into()));
            cfg.into()
        })
    }

    fn handle(
        &self,
        node_id: NodeId,
        conn: Connection,
        initiated: bool,
    ) -> BoxFuture<'static, Result<EchoConnection>> {
        let mapping = self.0.clone();

        async move {
            if conn.alpn() != Some(ALPN_ECHO.to_vec()) {
                anyhow::bail!("expected ALPN {:?}, got {:?}", ALPN_ECHO, conn.alpn());
            }

            let echo_conn = if initiated {
                let id = conn.stable_id() as u64;
                conn.open_uni().await?.write_all(&id.to_be_bytes()).await?;
                EchoConnection {
                    conn: conn.clone(),
                    id,
                }
            } else {
                let mut buf = [0; size_of::<u64>()];
                conn.accept_uni().await?.read_exact(&mut buf).await?;
                let id = u64::from_be_bytes(buf);
                EchoConnection {
                    conn: conn.clone(),
                    id,
                }
            };

            tokio::spawn(async move {
                while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                    if let Some(mapping) = mapping.clone() {
                        let mut lock = mapping.lock().await;
                        let event = Event::new(
                            node_id,
                            EventType::AcceptStream {
                                from: conn.remote_node_id().unwrap(),
                                conn: conn.stable_id(),
                            },
                        );
                        crate::event::emit_event(event, node_id, &mut lock, None);
                    }

                    tracing::info!(send = ?send.id(), recv = ?recv.id(), "[accept] BEGIN");

                    tokio::time::sleep(ECHO_DELAY).await;

                    let buf = recv.read_to_end(10_000).await?;
                    tracing::info!("[accept] received msg {}", std::str::from_utf8(&buf)?);

                    send.write_all(&buf).await?;
                    tracing::info!("[accept] replied with msg {}", std::str::from_utf8(&buf)?);

                    send.finish()?;
                    tracing::info!("[accept] DONE");

                    if let Some(mapping) = mapping.clone() {
                        let mut lock = mapping.lock().await;
                        let event = Event::new(
                            node_id,
                            EventType::EndStream {
                                to: conn.remote_node_id().unwrap(),
                                conn: conn.stable_id(),
                            },
                        );
                        crate::event::emit_event(event, node_id, &mut lock, None);
                    }
                }
                tracing::info!("handler loop for conn {} closing", conn.stable_id());
                anyhow::Ok(())
            });

            Ok(echo_conn)
        }
        .boxed()
    }
}
