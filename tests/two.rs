use std::time::Duration;

use anyhow::Result;
use futures::{FutureExt, future};
use iroh::{
    Endpoint,
    endpoint::Connection,
    protocol::{ProtocolHandler, Router},
};
use iroh_conn::ConnectionManager;

/// Artificial "processing time" delay for the echo handler
const ECHO_DELAY: Duration = Duration::from_millis(100);

#[tokio::test(flavor = "multi_thread")]
async fn test_two() -> Result<()> {
    let n1 = new_node().await?;
    let n2 = new_node().await?;

    let calls = [n1.rpc(&n2, b"hello"), n2.rpc(&n1, b"hello")];

    let _ = future::join_all(calls)
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;

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
    pub async fn new(endpoint: Endpoint) -> Result<Self> {
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

    pub async fn connect(&self, n: &Self) -> Result<Connection> {
        let conn = self
            .get_or_open_connection(n.manager.endpoint().node_id(), ALPN)
            .await?;
        Ok(conn)
    }

    pub async fn rpc(&self, n: &Self, msg: &[u8]) -> Result<()> {
        let conn = self.connect(n).await?;
        let (mut send, mut recv) = conn.open_bi().await?;
        send.write_all(msg).await?;
        send.finish()?;
        let response = recv.read_to_end(10_000).await?;
        assert_eq!(&response, msg);
        Ok(())
    }
}

async fn new_node() -> Result<TestNode> {
    let endpoint = Endpoint::builder().discovery_local_network().bind().await?;
    Ok(TestNode::new(endpoint).await?)
}

#[derive(Clone, Debug, derive_more::Constructor)]
struct Echo {
    manager: ConnectionManager,
    delay: Duration,
}

impl Echo {
    async fn handler(self, conn: Connection) -> Result<()> {
        loop {
            let (mut send, mut recv) = conn.accept_bi().await?;
            tokio::time::sleep(self.delay).await;
            let _ = tokio::io::copy(&mut recv, &mut send).await?;
            send.finish()?;
        }
    }
}

impl ProtocolHandler for Echo {
    fn accept(&self, connection: Connection) -> n0_future::future::Boxed<Result<()>> {
        let manager = self.manager.clone();
        tokio::spawn(self.clone().handler(connection.clone()));
        async move { manager.handle_connection(connection).await }.boxed()
    }
}
