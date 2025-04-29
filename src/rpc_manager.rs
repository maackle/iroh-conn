//! A connection manager to ensure roughly one open connection between peers,
//! if possible.

use std::{
    collections::{BTreeMap, btree_map::Entry},
    fmt::Debug,
    future::Future,
    sync::Arc,
};

use anyhow::Result;
use iroh::{Endpoint, NodeAddr, NodeId, endpoint::Connection};
use n0_future::{Either, task};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info_span};

use crate::{
    Alpn, ConnectionHandler, ConnectionManager,
    event::{Event, EventMappingShared, EventType, EventTypeSystem},
    handler::ManagedConnection,
    testing::SharedConnection,
};

/// A connection manager.
///
/// Tries to de-duplicate connections between its endpoint and other nodes.
///
/// Instead of opening a new connection to another node it will return you
/// existing connections.
///
/// Unfortunately, it can't deduplicate connections to only a single one,
/// as we need to rely on the remote end to close redundant connections.
#[derive(derive_more::Debug)]
pub struct RpcManager {
    endpoint: Endpoint,
    connections: Arc<Mutex<Connections>>,
    alpns: Vec<Alpn>,

    #[debug(skip)]
    handlers: Mutex<BTreeMap<Alpn, Arc<dyn ConnectionHandler<SharedConnection>>>>,

    // Handling subtask cancellation, aborted on drop
    #[debug(skip)]
    cancel: CancellationToken,

    #[cfg(feature = "modeling")]
    #[debug(skip)]
    pub events: EventMappingShared,
}

#[async_trait::async_trait]
impl ConnectionManager<SharedConnection> for RpcManager {
    async fn get_or_open_connection(
        &self,
        remote_addr: impl Into<NodeAddr> + Clone + Send,
        alpn: &[u8],
    ) -> Result<SharedConnection> {
        if !self.alpns.contains(&alpn.to_vec()) {
            anyhow::bail!("ALPN not accepted by this manager: {:?}", alpn);
        }

        let remote_addr = remote_addr.into();
        let remote_node_id = remote_addr.node_id;

        let mut conns = self.connections.lock().await;

        let conn = match conns.entry(remote_node_id) {
            // No connection open for this - need to open a new connection
            Entry::Vacant(spot) => {
                tracing::trace!(
                    "opening new connection... {} -> {}",
                    self.endpoint.node_id(),
                    remote_node_id
                );
                let conn = self.open_connection(remote_node_id, alpn).await?;
                tracing::trace!(conn = conn.shared_id(), "opened connection");
                spot.insert(conn.clone());

                self.emit_event(remote_node_id, conn.shared_id(), EventType::OpenConnection)
                    .await;

                tracing::debug!(
                    conn = conn.shared_id(),
                    "opened and registered new connection"
                );
                conn
            }

            // We have both initiated a connection for this, but also accepted some - potentially close ours.
            Entry::Occupied(existing) => {
                let conn = if existing.get().initiated {
                    existing.get().clone()
                } else if self.prefer_initiated(remote_node_id) {
                    let conn = self.open_connection(remote_node_id, alpn).await?;
                    existing.remove().close(
                        Self::CLOSE_CONNECTION_SUPERSEDED_CODE.into(),
                        &Self::CLOSE_CONNECTION_SUPERSEDED_MSG,
                    );
                    conns.insert(remote_node_id, conn.clone());
                    conn
                } else {
                    todo!();
                    conn
                };

                match self.preferred_connection(existing.get().clone(), conn.clone())? {
                    Either::Left(c) => {
                        tracing::debug!(conn = c.shared_id(), "reusing existing connection");
                        existing.remove().close(
                            Self::CLOSE_CONNECTION_SUPERSEDED_CODE.into(),
                            &Self::CLOSE_CONNECTION_SUPERSEDED_MSG,
                        );
                        c
                    }
                    Either::Right(c) => {
                        tracing::debug!(conn = c.shared_id(), "using new connection");
                        c
                    }
                }
                if !self.prefer_initiated(remote_node_id) {
                    initiated_conn.remove().close(
                        Self::CLOSE_CONNECTION_SUPERSEDED_CODE.into(),
                        &Self::CLOSE_CONNECTION_SUPERSEDED_MSG,
                    );

                    let best_conn = accepted_conns
                        .get()
                        .values()
                        // Filter out closed connections as a best-effort in case they were closed while we were holding the lock
                        .filter(|conn| conn.close_reason().is_none())
                        // Hmm. Using "lowest RTT" as an arbitrary measure now.
                        .min_by_key(|conn| conn.rtt())
                        .expect("always one conn in ConnectionSet entry")
                        .clone();
                    tracing::debug!(
                        conn = best_conn.shared_id(),
                        "closing initiated connection to use accepted connection"
                    );
                    best_conn
                } else {
                    let conn = initiated_conn.get().clone();
                    tracing::debug!(conn = conn.shared_id(), "keeping initiated connection");
                    conn
                }
            }
        };
        // tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        Ok(conn)
    }

    async fn handle_incoming_connection(&self, conn: Connection) -> Result<()> {
        tracing::debug!(conn = conn.shared_id(), "handling incoming connection");
        let remote_node_id = conn.remote_node_id()?;

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let alpn = conn
            .alpn()
            .ok_or_else(|| anyhow::anyhow!("Not tracking connections without ALPNs"))?;

        let handler = self
            .get_handler(&alpn)
            .await
            .map_err(|_| anyhow::anyhow!("No handler registered for ALPN: {:?}", alpn))?;

        // Now that we have the connection we wish to use, spawn the handler for it
        let conn = {
            handler
                .confirm(self.endpoint().node_id(), conn.clone(), false)
                .await?
        };

        let mut conns = self.connections.lock().await;
        if let Err(_) = conns
            .accepted
            .insert(remote_node_id, alpn.clone(), conn.clone())
        {
            // Reject any incoming connection attempts over the connection limit
            conn.close(
                Self::CLOSE_CONNECTION_LIMIT_EXCEEDED_CODE.into(),
                &Self::CLOSE_CONNECTION_LIMIT_EXCEEDED_MSG,
            );
            return Ok(());
        }

        self.emit_event(
            remote_node_id,
            conn.shared_id(),
            EventType::AcceptConnection,
        )
        .await;

        // If we had an open connection like this already, close it.
        if let Entry::Occupied(initiated_conn) =
            conns.initiated.entry((remote_node_id, alpn.clone()))
        {
            if !self.prefer_initiated(remote_node_id) {
                let closed = initiated_conn.remove();
                closed.close(
                    Self::CLOSE_CONNECTION_SUPERSEDED_CODE.into(),
                    &Self::CLOSE_CONNECTION_SUPERSEDED_MSG,
                );
                self.emit_event(remote_node_id, conn.shared_id(), EventType::CloseConnection)
                    .await;
            }
        }

        // Now that we have the connection we wish to use, spawn the handler for it
        let conn = {
            handler
                .handle(self.endpoint().node_id(), conn.clone(), false)
                .await?
        };

        // Listen to the remote end closing the connection:
        // see [a98sndiond]
        self.spawn_task(info_span!("observe_closed"), {
            let conns = self.connections.clone();
            let alpn = alpn.clone();
            async move {
                let close_err = conn.closed().await;
                tracing::debug!(
                    ?close_err,
                    "accepted conn closed, removing from connection set"
                );
                conns
                    .lock()
                    .await
                    .accepted
                    .remove(remote_node_id, alpn, &conn);
            }
        });
        Ok(())
    }
}

impl RpcManager {
    const CLOSE_CONNECTION_LIMIT_EXCEEDED_CODE: u32 = 10;
    const CLOSE_CONNECTION_LIMIT_EXCEEDED_MSG: &[u8] =
        b"ConnectionManager: Connection limit exceeded";

    const CLOSE_CONNECTION_SUPERSEDED_CODE: u32 = 11;
    const CLOSE_CONNECTION_SUPERSEDED_MSG: &[u8] = b"ConnectionManager: Connection superseded";

    /// TODO docs
    pub fn spawn(endpoint: Endpoint, events: EventMappingShared) -> Arc<Self> {
        let manager = Arc::new(Self {
            endpoint: endpoint.clone(),
            connections: Default::default(),
            handlers: Default::default(),
            cancel: CancellationToken::new(),
            events,
        });

        manager.spawn_task(info_span!("connection-accept-loop"), {
            let endpoint = endpoint.clone();
            let manager = manager.clone();
            async move {
                tracing::debug!(
                    me = endpoint.node_id().fmt_short(),
                    "ConnectionManager accept loop started"
                );

                while let Some(incoming) = endpoint.accept().await {
                    tracing::info!("ConnectionManager accepted connection");
                    let conn = incoming.await?;
                    manager.handle_incoming_connection(conn).await?;
                }
                tracing::warn!("ConnectionManager accept loop exited");
                anyhow::Ok(())
            }
        });

        manager
    }

    // pub fn with_event_mapping(mut self, mapper: EventMappingShared) -> Self {
    //     self.events = Some(mapper);
    //     self
    // }

    pub async fn register_handler(
        &self,
        alpns: Vec<Alpn>,
        handler: impl ConnectionHandler<SharedConnection>,
    ) -> Result<()> {
        let handler = Arc::new(handler);
        let mut lock = self.handlers.lock().await;
        for alpn in alpns.iter() {
            if lock.contains_key(alpn) {
                anyhow::bail!("ALPN already registered: {:?}", alpn);
            }
        }
        for alpn in alpns.clone() {
            lock.insert(alpn, handler.clone());
        }
        tracing::info!("Registered handler for ALPNs: {:?}", alpns);
        Ok(())
    }

    async fn open_connection(
        &self,
        remote_node_id: NodeId,
        alpn: &[u8],
    ) -> Result<SharedConnection> {
        let handler = self.get_handler(alpn).await?;
        tracing::trace!(
            "using handler to open connection {} -> {}, alpn={:?}",
            self.endpoint.node_id(),
            remote_node_id,
            alpn
        );
        // let conn = self.endpoint.connect(remote_node_id, &alpn).await?;
        let conn = handler
            .open(self.endpoint.clone(), remote_node_id, alpn.to_vec())
            .await?;

        let conn = handler
            .confirm(self.endpoint().node_id(), conn.clone(), true)
            .await?;

        self.spawn_task(
            info_span!("open_connection handler"),
            handler.handle(self.endpoint().node_id(), conn.clone(), true),
        );
        Ok(conn)
    }

    async fn get_handler(
        &self,
        alpn: &[u8],
    ) -> Result<Arc<dyn ConnectionHandler<SharedConnection>>> {
        Ok(self
            .handlers
            .lock()
            .await
            .get(alpn)
            .ok_or_else(|| anyhow::anyhow!("No handler registered for ALPN: {:?}", alpn))?
            .clone())
    }

    pub async fn emit_event(&self, remote: NodeId, conn: u64, event_type: EventTypeSystem) {
        if let Some(events) = &self.events {
            let mut lock = events.lock().await;
            let event = Event::new(self.endpoint.node_id(), remote, conn, event_type);
            crate::event::emit_event(event, &mut lock)
        }
    }

    fn spawn_task<F>(&self, span: tracing::Span, task: F)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let token = self.cancel.clone();
        task::spawn(async move { token.run_until_cancelled(task).await }.instrument(span));
    }

    fn prefer_initiated(&self, remote_node_id: NodeId) -> bool {
        let node_id = self.endpoint.node_id();
        let our_way = blake3::hash(&[*node_id.as_bytes(), *remote_node_id.as_bytes()].concat());
        let remote_way = blake3::hash(&[*remote_node_id.as_bytes(), *node_id.as_bytes()].concat());
        our_way.as_bytes() < remote_way.as_bytes()
    }

    fn preferred_connection(
        &self,
        c1: SharedConnection,
        c2: SharedConnection,
    ) -> Result<Either<SharedConnection, SharedConnection>> {
        let remote = c1.remote_node_id()?;
        debug_assert_eq!(remote, c2.remote_node_id()?);
        debug_assert!(c1.initiated != c2.initiated);
        let prefer_initiated = self.prefer_initiated(remote);
        if prefer_initiated == c1.initiated {
            Ok(Either::Left(c1))
        } else {
            Ok(Either::Right(c2))
        }
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }
}

impl Drop for RpcManager {
    fn drop(&mut self) {
        self.cancel.cancel();
        // quinn Connections will close automatically when dropped.
    }
}

// Private

type Connections = BTreeMap<NodeId, SharedConnection>;
