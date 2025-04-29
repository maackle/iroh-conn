use std::{sync::Arc, time::Duration};

use crate::{
    ConnectionHandler, ConnectionManager,
    event::{Event, EventMappingShared, EventType},
    handler::ManagedConnection,
    matheus::BasicConnectionManager,
    testing::discover,
};
use anyhow::{Context, Result};
use futures::{
    FutureExt,
    future::{self, BoxFuture},
};
use iroh::{
    Endpoint, NodeId,
    endpoint::{ConnectOptions, Connection, TransportConfig, VarInt},
};

pub const ALPN_ECHO: &[u8] = b"test/echo";

#[derive(Clone, Debug, derive_more::Deref)]
pub struct TestNode {
    #[deref]
    manager: Arc<BasicConnectionManager>,
}

impl TestNode {
    pub async fn new(
        handler: impl ConnectionHandler<SharedConnection>,
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
        handler: impl ConnectionHandler<SharedConnection> + Clone,
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
    pub async fn connect(&self, n: &Self, alpn: &[u8]) -> Result<SharedConnection> {
        let conn = self
            .get_or_open_connection(n.manager.endpoint().node_id(), alpn)
            .await?;
        Ok(conn)
    }

    async fn rpc_inner(&self, n: &Self, msg: &[u8], conn: SharedConnection) -> Result<Vec<u8>> {
        let (mut send, mut recv) = conn.open_bi().await?;
        self.manager
            .emit_event(
                n.endpoint().node_id(),
                conn.shared_id(),
                EventType::OpenStream { stream: send.id() },
            )
            .await;

        tracing::debug!(send = ?send.id(), recv = ?recv.id(), "[caller]");
        send.write_all(msg).await?;
        tracing::debug!("[caller] wrote data: {}", std::str::from_utf8(msg)?);
        self.manager
            .emit_event(
                n.endpoint().node_id(),
                conn.shared_id(),
                EventType::WriteStream { stream: send.id() },
            )
            .await;

        send.finish()?;
        tracing::debug!("[caller] finished sending");
        self.manager
            .emit_event(
                n.endpoint().node_id(),
                conn.shared_id(),
                EventType::FinishStream { stream: send.id() },
            )
            .await;

        let response = recv.read_to_end(10_000).await?;
        tracing::info!("[caller] DONE");
        self.manager
            .emit_event(
                n.endpoint().node_id(),
                conn.shared_id(),
                EventType::ReadStream { stream: send.id() },
            )
            .await;

        assert_eq!(msg, &response);

        self.manager
            .emit_event(
                n.endpoint().node_id(),
                conn.shared_id(),
                EventType::EndStream { stream: send.id() },
            )
            .await;

        Ok(response)
    }

    /// Make a single RPC to the given node
    pub async fn rpc(&self, n: &Self, msg: &[u8]) -> Result<Vec<u8>> {
        tracing::info!(
            "[caller] BEGIN {} -> {}",
            self.endpoint().node_id().fmt_short(),
            n.endpoint().node_id().fmt_short()
        );
        let conn = self
            .connect(n, ALPN_ECHO)
            .await
            .context("rpc() couldn't establish connection")?;

        match self.rpc_inner(n, msg, conn.clone()).await {
            Ok(response) => Ok(response),
            Err(e) => {
                #[cfg(feature = "modeling")]
                {
                    self.manager
                        .emit_event(
                            n.endpoint().node_id(),
                            conn.shared_id(),
                            EventType::Error { err: e.to_string() },
                        )
                        .await;

                    let mut ev = self.manager.events.as_ref().unwrap().lock().await;
                    let i = ev.nodes.lookup(self.endpoint().node_id()).unwrap();
                    let j = ev.nodes.lookup(n.endpoint().node_id()).unwrap();
                    let c = ev.conns.lookup(conn.shared_id()).unwrap();
                    Err(anyhow::anyhow!("rpc error: {e}, {i} -/-> {j} : {c}"))
                }

                #[cfg(not(feature = "modeling"))]
                {
                    Err(e)
                }
            }
        }
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
            calls.push(async move { n.rpc(&m, msg).await });
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

#[derive(Clone, derive_more::Constructor)]
pub struct EchoHandler(EventMappingShared);

#[derive(Clone, Debug, derive_more::Deref, derive_more::Constructor)]
pub struct SharedConnection {
    pub id: u64,
    pub initiated: bool,

    #[deref]
    pub conn: Connection,
}

impl ManagedConnection for SharedConnection {
    fn shared_id(&self) -> u64 {
        self.id
    }
}

impl ConnectionHandler<SharedConnection> for EchoHandler {
    fn connect_options(&self) -> ConnectOptions {
        // 1 second idle timeout for these tests
        ConnectOptions::new().with_transport_config({
            let mut cfg = TransportConfig::default();
            cfg.max_idle_timeout(Some(VarInt::from_u32(1_000).into()));
            cfg.into()
        })
    }

    fn confirm(
        &self,
        _node_id: NodeId,
        conn: Connection,
        initiated: bool,
    ) -> BoxFuture<'static, Result<SharedConnection>> {
        async move {
            if conn.alpn() != Some(ALPN_ECHO.to_vec()) {
                anyhow::bail!("expected ALPN {:?}, got {:?}", ALPN_ECHO, conn.alpn());
            }

            let conn = if initiated {
                let id = conn.stable_id() as u64;
                conn.open_uni().await?.write_all(&id.to_be_bytes()).await?;
                SharedConnection {
                    conn: conn.clone(),
                    initiated,
                    id,
                }
            } else {
                let mut buf = [0; size_of::<u64>()];
                conn.accept_uni().await?.read_exact(&mut buf).await?;
                let id = u64::from_be_bytes(buf);
                SharedConnection {
                    conn: conn.clone(),
                    initiated,
                    id,
                }
            };

            Ok(conn)
        }
        .boxed()
    }

    fn handle(
        &self,
        node_id: NodeId,
        conn: SharedConnection,
        _initiated: bool,
    ) -> BoxFuture<'static, Result<SharedConnection>> {
        let mapping = self.0.clone();

        async move {
            let echo_conn = conn.clone();

            tokio::spawn(async move {
                while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                    let mapping = mapping.clone();
                    let emit = |event_type| async {
                        if let Some(mapping) = mapping.clone() {
                            let mut lock = mapping.lock().await;
                            let event = Event::new(
                                node_id,
                                conn.remote_node_id().unwrap(),
                                conn.shared_id(),
                                event_type,
                            );
                            crate::event::emit_event(event, &mut lock);
                        }
                    };

                    emit(EventType::AcceptStream { stream: send.id() }).await;

                    tracing::info!(send = ?send.id(), recv = ?recv.id(), "[accept] BEGIN");

                    tokio::time::sleep(ECHO_DELAY).await;

                    let buf = recv.read_to_end(10_000).await?;
                    tracing::info!("[accept] received msg {}", std::str::from_utf8(&buf)?);
                    emit(EventType::ReadStream { stream: send.id() }).await;

                    send.write_all(&buf).await?;
                    tracing::info!("[accept] replied with msg {}", std::str::from_utf8(&buf)?);
                    emit(EventType::WriteStream { stream: send.id() }).await;

                    send.finish()?;
                    tracing::info!("[accept] DONE");
                    emit(EventType::FinishStream { stream: send.id() }).await;

                    emit(EventType::EndStream { stream: send.id() }).await;
                }
                tracing::info!("handler loop for conn {} closing", conn.shared_id());
                anyhow::Ok(())
            });

            Ok(echo_conn)
        }
        .boxed()
    }
}
