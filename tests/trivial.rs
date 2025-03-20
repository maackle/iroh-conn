use std::{sync::Arc, time::Duration};

use anyhow::Result;
use futures::future::join_all;
use iroh::{Endpoint, NodeId, endpoint::Connection};
use iroh_conn::testing::{await_fully_connected, setup_tracing};
use tokio::sync::Mutex;

const ALPN: &[u8] = b"trivial";
const ECHO_DELAY: Duration = Duration::from_millis(100);

#[tokio::test(flavor = "multi_thread")]
async fn trivial() -> Result<()> {
    setup_tracing("off,trivial=info");

    let e1 = Node::spawn().await;
    let e2 = Node::spawn().await;

    await_fully_connected(vec![e1.endpoint.clone(), e2.endpoint.clone()]).await;

    println!("\nCALL 1 -> 2\n");
    e1.send(e2.node_id(), "aloha").await?;

    println!("\nCALL 2 -> 1\n");
    e2.send(e1.node_id(), "buongiorno").await?;

    // NOTE: this hangs up
    println!("\nSIMULTANEOUS 1 <-> 2\n");
    join_all({
        [
            tokio::spawn({
                let e1 = e1.clone();
                let e2 = e2.clone();
                async move { e1.send(e2.node_id(), "ciao").await }
            }),
            tokio::spawn({
                let e1 = e1.clone();
                let e2 = e2.clone();
                async move { e2.send(e1.node_id(), "dia dhuit").await }
            }),
        ]
    })
    .await
    .into_iter()
    .collect::<Result<Vec<_>, _>>()?;

    Ok(())
}

/// A node which can hold exactly zero or one connections.
#[derive(Debug, Clone)]
pub struct Node {
    pub endpoint: Endpoint,
    pub conn: OneConn,
}

impl Node {
    /// Create a new node and spawn the connection accept loop.
    pub async fn spawn() -> Self {
        let endpoint = Endpoint::builder()
            .alpns(vec![ALPN.to_vec()])
            .discovery_local_network()
            .bind()
            .await
            .unwrap();

        let conn = OneConn::default();

        // Connection accept loop
        tokio::spawn({
            let oneconn = conn.clone();
            let endpoint = endpoint.clone();
            async move {
                while let Some(incoming) = endpoint.accept().await {
                    let conn = incoming.await?;
                    oneconn.set(conn).await;
                }
                anyhow::Ok(())
            }
        });

        Self { endpoint, conn }
    }

    /// Send a message to the target node and assert that the response matches what was sent.
    pub async fn send(&self, target: NodeId, msg: &str) -> Result<()> {
        let conn = self.connect(target).await?;

        let (mut send, mut recv) = conn.open_bi().await?;

        tracing::debug!(send = ?send.id(), recv = ?recv.id(), "[caller]");

        send.write_all(msg.as_bytes()).await?;

        tracing::debug!("[caller] wrote data: {}", msg);

        send.finish()?;

        tracing::debug!("[caller] finished sending");

        let response = recv.read_to_end(10_000).await?;

        tracing::info!("[caller] DONE");

        assert_eq!(msg.as_bytes(), &response);
        Ok(())
    }

    /// Return a connection to the target node, opening a new connection if necessary.
    pub async fn connect(&self, target: NodeId) -> Result<Connection> {
        match self.conn.get().await {
            Some(conn) => Ok(conn),
            None => {
                let conn = self.endpoint.connect(target, ALPN).await?;
                self.conn.set(conn.clone()).await;
                Ok(conn)
            }
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.endpoint.node_id()
    }
}

/// A "connection manager" that ensures there is at most one connection at a time.
#[derive(Debug, Default, Clone)]
pub struct OneConn(Arc<Mutex<Option<Connection>>>);

impl OneConn {
    /// Return the current connection, if any.
    pub async fn get(&self) -> Option<Connection> {
        self.0.lock().await.clone()
    }

    /// Set the current connection and spawn the echo protocol handler.
    pub async fn set(&self, conn: Connection) {
        let mut lock = self.0.lock().await;

        if lock.is_some() {
            panic!("connection already set");
        }

        tokio::spawn({
            let conn = conn.clone();
            async move {
                let (mut send, mut recv) = conn.accept_bi().await?;

                tracing::info!(send = ?send.id(), recv = ?recv.id(), "[accept] BEGIN");

                let buf = recv.read_to_end(10_000).await?;

                tracing::info!("[accept] received msg {}", std::str::from_utf8(&buf)?);

                tokio::time::sleep(ECHO_DELAY).await;
                send.write_all(&buf).await?;

                tracing::info!("[accept] replied with msg {}", std::str::from_utf8(&buf)?);

                send.finish()?;

                tracing::info!("[accept] DONE");

                anyhow::Ok(())
            }
        });

        *lock = Some(conn);
    }
}
