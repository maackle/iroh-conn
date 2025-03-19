use std::time::Duration;

use anyhow::Result;
use futures::{FutureExt, future};
use iroh::{
    Endpoint,
    endpoint::{Connecting, Connection},
    protocol::{ProtocolHandler, Router},
};
use iroh_conn::{ConnectionManager, testing::await_fully_connected};
use tracing::Level;

/// Artificial "processing time" delay for the echo handler
const ECHO_DELAY: Duration = Duration::from_millis(1);

#[tokio::test(flavor = "multi_thread")]
async fn test_two() -> Result<()> {
    tracing_subscriber::fmt::fmt()
        .with_file(true)
        .with_line_number(true)
        // .with_max_level(Level::DEBUG)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_new("two=debug,iroh_conn=trace").unwrap(),
        )
        .init();

    let n1 = TestNode::new().await?;
    let n2 = TestNode::new().await?;
    await_fully_connected([n1.endpoint().clone(), n2.endpoint().clone()]).await;

    // let calls = [n1.rpc(&n2, b"hello"), n2.rpc(&n1, b"hello")];

    // let result = future::join_all(calls)
    //     .await
    //     .into_iter()
    //     .collect::<Result<Vec<_>>>();

    // assert!(result.is_err());

    println!("\nCALL 1 -> 2\n");
    n1.rpc(&n2, b"aloha").await?;

    println!("\nCALL 2 -> 1\n");
    n2.rpc(&n1, b"buongiorno").await?;

    Ok(())
}

const ALPN: &[u8] = b"iroh-conn";

#[derive(Clone, derive_more::Deref)]
pub struct TestNode {
    #[deref]
    manager: ConnectionManager,
    _router: Router,
}

impl TestNode {
    pub async fn new() -> Result<Self> {
        let endpoint = Endpoint::builder().discovery_local_network().bind().await?;
        let manager = ConnectionManager::new(endpoint.clone());
        let router = Router::builder(endpoint)
            .accept(ALPN, Echo::new(manager.clone(), ECHO_DELAY))
            .spawn()
            .await?;
        Ok(Self {
            manager,
            _router: router,
        })
    }

    #[tracing::instrument(skip_all, fields(node = self.endpoint().node_id().fmt_short(), remote = n.endpoint().node_id().fmt_short()))]
    pub async fn connect(&self, n: &Self) -> Result<Connection> {
        let conn = self
            .get_or_open_connection(n.manager.endpoint().node_id(), ALPN)
            .await?;
        Ok(conn)
    }

    #[tracing::instrument(skip_all, fields(node = self.endpoint().node_id().fmt_short(), remote = n.endpoint().node_id().fmt_short()))]
    pub async fn rpc(&self, n: &Self, msg: &[u8]) -> Result<()> {
        tracing::info!("[caller] BEGIN");
        let conn = self.connect(n).await?;
        let (mut send, mut recv) = conn.open_bi().await?;
        tracing::debug!(send = ?send.id(), recv = ?recv.id(), "[caller]");
        send.write_all(msg).await?;
        tracing::debug!("[caller] wrote data");
        send.finish()?;
        tracing::debug!("[caller] finished sending");
        let response = recv.read_to_end(10_000).await?;
        assert_eq!(&response, msg);
        tracing::info!("[caller] DONE");
        Ok(())
    }
}

#[derive(Clone, Debug, derive_more::Constructor)]
struct Echo {
    manager: ConnectionManager,
    delay: Duration,
}

impl Echo {
    async fn handler(self, conn: Connection) -> Result<()> {
        while let Ok((mut send, mut recv)) = conn.accept_bi().await {
            tracing::info!(send = ?send.id(), recv = ?recv.id(), "[accept] BEGIN");
            tokio::time::sleep(self.delay).await;
            let _ = tokio::io::copy(&mut recv, &mut send).await?;
            send.finish()?;
            tracing::info!("[accept] DONE");
        }
        tracing::info!("handler loop for conn {} closing", conn.stable_id());
        Ok(())
    }
}

impl ProtocolHandler for Echo {
    fn accept(&self, connecting: Connecting) -> n0_future::future::Boxed<Result<()>> {
        let echo = self.clone();
        async move {
            let connection = connecting.await?;
            let remote = connection.remote_node_id()?;
            echo.manager
                .clone()
                .handle_incoming_connection(connection)
                .await?;
            let best_conn = echo.manager.get_or_open_connection(remote, ALPN).await?;
            tokio::spawn(echo.clone().handler(best_conn));
            Ok(())
        }
        .boxed()
    }
}
