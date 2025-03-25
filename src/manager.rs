//! A connection manager to ensure roughly one open connection between peers,
//! if possible.

use std::{
    collections::{BTreeMap, btree_map::Entry},
    future::Future,
    sync::Arc,
};

use anyhow::Result;
use iroh::{Endpoint, NodeAddr, NodeId, endpoint::Connection};
use n0_future::task;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info_span};

use crate::{Alpn, ConnectionHandler};

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
pub struct ConnectionManager {
    endpoint: Endpoint,
    connections: Arc<Mutex<Connections>>,

    #[debug(skip)]
    handlers: Arc<Mutex<BTreeMap<Alpn, Arc<dyn ConnectionHandler>>>>,

    // Handling subtask cancellation, aborted on drop
    cancel: CancellationToken,
}

impl ConnectionManager {
    const CLOSE_CONNECTION_LIMIT_EXCEEDED_CODE: u32 = 10;
    const CLOSE_CONNECTION_LIMIT_EXCEEDED_MSG: &[u8] =
        b"ConnectionManager: Connection limit exceeded";

    const CLOSE_CONNECTION_SUPERSEDED_CODE: u32 = 11;
    const CLOSE_CONNECTION_SUPERSEDED_MSG: &[u8] = b"ConnectionManager: Connection superseded";

    /// TODO docs
    pub fn spawn(endpoint: Endpoint) -> Arc<Self> {
        let manager = Arc::new(Self {
            endpoint: endpoint.clone(),
            connections: Default::default(),
            handlers: Default::default(),
            cancel: CancellationToken::new(),
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

    pub async fn register_handler(
        &self,
        alpns: Vec<Alpn>,
        handler: impl ConnectionHandler,
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

    /// TODO docs
    #[tracing::instrument(skip_all, fields(node = self.endpoint().node_id().fmt_short(), remote = conn.remote_node_id()?.fmt_short()))]
    pub async fn handle_incoming_connection(&self, conn: Connection) -> Result<()> {
        tracing::debug!(conn = conn.stable_id(), "handling incoming connection");
        let remote_node_id = conn.remote_node_id()?;

        let alpn = conn
            .alpn()
            .ok_or_else(|| anyhow::anyhow!("Not tracking connections without ALPNs"))?;

        if !self.handlers.lock().await.contains_key(&alpn) {
            anyhow::bail!("No handler registered for ALPN: {:?}", alpn);
        }

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

        // If we had an open connection like this already, close it.
        if let Entry::Occupied(initiated_conn) =
            conns.initiated.entry((remote_node_id, alpn.clone()))
        {
            if !self.prefer_initiated(remote_node_id) {
                initiated_conn.remove().close(
                    Self::CLOSE_CONNECTION_SUPERSEDED_CODE.into(),
                    &Self::CLOSE_CONNECTION_SUPERSEDED_MSG,
                );
            }
        }

        // Now that we have the connection we wish to use, spawn the handler for it
        {
            let handler = self.get_handler(&alpn).await?;
            self.spawn_task(
                info_span!("handler accept loop"),
                handler.handle(conn.clone()),
            );
        }

        // Listen to the remote end closing the connection:
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

    /// TODO docs
    // #[tracing::instrument(skip_all, fields(node = self.endpoint().node_id().fmt_short(), remote = remote_addr.clone().into().node_id.fmt_short()))]
    pub async fn get_or_open_connection(
        &self,
        remote_addr: impl Into<NodeAddr> + Clone,
        alpn: &[u8],
    ) -> Result<Connection> {
        let remote_addr = remote_addr.into();
        let remote_node_id = remote_addr.node_id;

        let mut conns = self.connections.lock().await;

        let Connections {
            initiated,
            accepted,
        } = &mut *conns;
        let initiated_conn = initiated.entry((remote_node_id, alpn.to_vec()));
        // If we already have an accepted connection & prefer that, reuse that one
        let accepted_conns = accepted.get_conns(remote_node_id, alpn.to_vec());
        let conn = match (initiated_conn, accepted_conns) {
            // No connection open for this - need to open a new connection
            (Entry::Vacant(spot), Entry::Vacant(_)) => {
                tracing::trace!(
                    "opening new connection... {} -> {}",
                    self.endpoint.node_id(),
                    remote_node_id
                );
                let conn = self.open_connection(remote_node_id, alpn).await?;
                tracing::trace!(conn = conn.stable_id(), "opened connection");
                spot.insert(conn.clone());
                tracing::debug!(
                    conn = conn.stable_id(),
                    "opened and registered new connection"
                );
                conn
            }

            // We have accepted connections for this - re-use them.
            (Entry::Vacant(_), Entry::Occupied(accepted_conns)) => {
                tracing::trace!("reusing accepted connection...");
                let conn = accepted_conns
                    .get()
                    .values()
                    // Filter out closed connections as a best-effort in case they were closed while we were holding the lock
                    .filter(|conn| conn.close_reason().is_none())
                    // Hmm. Using "lowest RTT" as an arbitrary measure now.
                    .min_by_key(|conn| conn.rtt())
                    .expect("always one conn in ConnectionSet entry")
                    .clone();
                tracing::debug!(conn = conn.stable_id(), "reusing accepted connection");
                conn
            }

            // We have already initiated a connection for this - reuse it.
            (Entry::Occupied(initiated_conn), Entry::Vacant(_)) => {
                let conn = initiated_conn.get().clone();
                tracing::debug!(conn = conn.stable_id(), "reusing initiated connection");
                conn
            }

            // We have both initiated a connection for this, but also accepted some - potentially close ours.
            (Entry::Occupied(initiated_conn), Entry::Occupied(accepted_conns)) => {
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
                        conn = best_conn.stable_id(),
                        "closing initiated connection to use accepted connection"
                    );
                    best_conn
                } else {
                    let conn = initiated_conn.get().clone();
                    tracing::debug!(conn = conn.stable_id(), "keeping initiated connection");
                    conn
                }
            }
        };

        Ok(conn)
    }

    async fn open_connection(&self, remote_node_id: NodeId, alpn: &[u8]) -> Result<Connection> {
        dbg!();
        let handler = self.get_handler(alpn).await?;
        dbg!();
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
        dbg!();

        self.spawn_task(
            info_span!("open_connection handler"),
            handler.handle(conn.clone()),
        );
        Ok(conn)
    }

    async fn get_handler(&self, alpn: &[u8]) -> Result<Arc<dyn ConnectionHandler>> {
        Ok(self
            .handlers
            .lock()
            .await
            .get(alpn)
            .ok_or_else(|| anyhow::anyhow!("No handler registered for ALPN: {:?}", alpn))?
            .clone())
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

    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }
}

impl Drop for ConnectionManager {
    fn drop(&mut self) {
        self.cancel.cancel();
        // quinn Connections will close automatically when dropped.
    }
}

// Private

#[derive(Debug, Default)]
struct Connections {
    initiated: BTreeMap<(NodeId, Alpn), Connection>,
    accepted: ConnectionSet,
}

#[derive(Debug, Default)]
struct ConnectionSet {
    inner: BTreeMap<(NodeId, Alpn), BTreeMap<usize, Connection>>,
}

impl ConnectionSet {
    pub fn insert(&mut self, node_id: NodeId, alpn: Alpn, conn: Connection) -> Result<()> {
        let conns = self.inner.entry((node_id, alpn)).or_default();
        const CONN_LIMIT: usize = 5;
        anyhow::ensure!(conns.len() <= CONN_LIMIT, "Connection limit exceeded");
        conns.insert(conn.stable_id(), conn);
        Ok(())
    }

    pub fn remove(&mut self, node_id: NodeId, alpn: Alpn, conn: &Connection) {
        if let Entry::Occupied(mut entry) = self.inner.entry((node_id, alpn)) {
            entry.get_mut().remove(&conn.stable_id());
            if entry.get().is_empty() {
                entry.remove();
            }
        }
    }

    pub fn get_conns(
        &mut self,
        node_id: NodeId,
        alpn: Alpn,
    ) -> Entry<'_, (NodeId, Alpn), BTreeMap<usize, Connection>> {
        self.inner.entry((node_id, alpn))
    }
}
