use std::sync::Arc;

use crate::{BasicConnectionManager, ConnectionHandler, ConnectionManager, testing::discover};
use anyhow::Result;
use futures::future;
use iroh::{Endpoint, endpoint::Connection};

pub const ALPN_ECHO: &[u8] = b"test/echo";

#[derive(Clone, Debug, derive_more::Deref)]
pub struct TestNode {
    #[deref]
    manager: Arc<BasicConnectionManager>,
}

impl TestNode {
    pub async fn new(
        handler: impl ConnectionHandler,
        alpns: impl IntoIterator<Item = Vec<u8>>,
    ) -> Result<Self> {
        let alpns = alpns.into_iter().collect::<Vec<_>>();
        let endpoint = Endpoint::builder().alpns(alpns.clone()).bind().await?;
        let manager = BasicConnectionManager::spawn(endpoint.clone());
        manager.register_handler(alpns, handler).await?;
        Ok(Self { manager })
    }

    pub async fn cluster<const N: usize>(
        handler: impl ConnectionHandler + Clone,
        alpns: impl IntoIterator<Item = Vec<u8>>,
    ) -> Result<[Self; N]> {
        let alpns = alpns.into_iter().collect::<Vec<_>>();
        let nodes = future::join_all(
            std::iter::repeat_with(|| TestNode::new(handler.clone(), alpns.clone())).take(N),
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

    // #[tracing::instrument(skip_all, fields(node = self.endpoint().node_id().fmt_short(), remote = n.endpoint().node_id().fmt_short()))]
    pub async fn rpc(&self, n: &Self, msg: &[u8]) -> Result<Vec<u8>> {
        tracing::info!("[caller] BEGIN");
        let conn = self.connect(n, ALPN_ECHO).await?;
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
