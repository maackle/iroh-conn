use std::time::Duration;

use anyhow::Result;
use futures::{
    FutureExt,
    future::{self, BoxFuture},
};
use iroh::{Endpoint, endpoint::Connection};
use iroh_conn::{
    ConnectionHandler, ConnectionManager,
    testing::{await_fully_connected, setup_tracing},
};

const ALPN: &[u8] = b"iroh-conn-test";

/// Artificial "processing time" delay for the echo handler
const ECHO_DELAY: Duration = Duration::from_millis(100);

#[tokio::test(flavor = "multi_thread")]
async fn test_two_simultaneous() -> Result<()> {
    setup_tracing("two=info,iroh_conn=info");

    let n1 = TestNode::new().await?;
    let n2 = TestNode::new().await?;
    println!("1: {}", n1.endpoint().node_id().fmt_short());
    println!("2: {}", n2.endpoint().node_id().fmt_short());
    await_fully_connected([n1.endpoint().clone(), n2.endpoint().clone()]).await;

    // First simultaneous call fails
    TestNode::rpc_cycle([&n1, &n2], b"hello").await.unwrap_err();
    // Second simultaneous call succeeds
    TestNode::rpc_cycle([&n1, &n2], b"hi").await.unwrap();

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_two_simultaneous_warmed() -> Result<()> {
    setup_tracing("two=info,iroh_conn=info");

    let n1 = TestNode::new().await?;
    let n2 = TestNode::new().await?;
    println!("1: {}", n1.endpoint().node_id().fmt_short());
    println!("2: {}", n2.endpoint().node_id().fmt_short());
    await_fully_connected([n1.endpoint().clone(), n2.endpoint().clone()]).await;

    // First "warm" the connections by having a clear opener and acceptor
    println!("\nCALL 1 -> 2\n");
    for _ in 0..2 {
        n1.rpc(&n2, b"aloha").await?;
    }

    println!("\nCALL 2 -> 1\n");
    for _ in 0..2 {
        n2.rpc(&n1, b"buongiorno").await?;
    }

    println!("\nSIMULTANEOUS 1 <-> 2\n");
    let calls = [
        n1.rpc_task(&n2, b"hello from 1"),
        n2.rpc_task(&n1, b"hello from 2"),
    ];
    let _result = future::join_all(calls)
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;

    Ok(())
}

#[derive(Clone, derive_more::Deref)]
pub struct TestNode {
    #[deref]
    manager: ConnectionManager,
}

impl TestNode {
    pub async fn new() -> Result<Self> {
        let endpoint = Endpoint::builder()
            .alpns(vec![ALPN.to_vec()])
            .discovery_local_network()
            .bind()
            .await?;
        let manager = ConnectionManager::new(endpoint.clone(), TestHandler);
        Ok(Self { manager })
    }

    #[tracing::instrument(skip_all, fields(node = self.endpoint().node_id().fmt_short(), remote = n.endpoint().node_id().fmt_short()))]
    pub async fn connect(&self, n: &Self) -> Result<Connection> {
        let conn = self
            .get_or_open_connection(n.manager.endpoint().node_id(), ALPN)
            .await?;
        Ok(conn)
    }

    // #[tracing::instrument(skip_all, fields(node = self.endpoint().node_id().fmt_short(), remote = n.endpoint().node_id().fmt_short()))]
    pub async fn rpc(&self, n: &Self, msg: &[u8]) -> Result<Vec<u8>> {
        tracing::info!("[caller] BEGIN");
        let conn = self.connect(n).await?;
        let (mut send, mut recv) = conn.open_bi().await?;
        tracing::debug!(send = ?send.id(), recv = ?recv.id(), "[caller]");
        send.write_all(msg).await?;
        tracing::debug!("[caller] wrote data: {}", std::str::from_utf8(msg)?);
        send.finish()?;
        tracing::debug!("[caller] finished sending");
        let response = recv.read_to_end(10_000).await?;
        tracing::info!("[caller] DONE");
        assert_eq!(msg, &response);
        Ok(response)
    }

    pub async fn rpc_cycle(ns: impl IntoIterator<Item = &Self>, msg: &[u8]) -> Result<()> {
        let ns = ns.into_iter().collect::<Vec<_>>();
        let num = ns.len();
        let mut calls = vec![];
        for i in 0..num {
            calls.push(ns[i].rpc(&ns[(i + 1) % num], msg));
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

    pub async fn rpc_task(&self, n: &Self, msg: &[u8]) -> Result<Vec<u8>> {
        let this = self.clone();
        let n = n.clone();
        let msg = msg.to_vec();
        tokio::spawn(async move { this.rpc(&n, &msg).await }).await?
    }
}

pub struct TestHandler;

impl ConnectionHandler for TestHandler {
    fn handle(&self, conn: Connection) -> BoxFuture<'static, Result<()>> {
        async move {
            while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                tracing::info!(send = ?send.id(), recv = ?recv.id(), "[accept] BEGIN");

                tokio::time::sleep(ECHO_DELAY).await;

                let buf = recv.read_to_end(10_000).await?;
                tracing::info!("[accept] received msg {}", std::str::from_utf8(&buf)?);

                send.write_all(&buf).await?;
                tracing::info!("[accept] replied with msg {}", std::str::from_utf8(&buf)?);

                send.finish()?;
                tracing::info!("[accept] DONE");
            }
            tracing::info!("handler loop for conn {} closing", conn.stable_id());
            Ok(())
        }
        .boxed()
    }
}
