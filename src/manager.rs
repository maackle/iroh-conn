//! A connection manager to ensure roughly one open connection between peers,
//! if possible.

use anyhow::Result;
use iroh::{NodeAddr, endpoint::Connection};

#[async_trait::async_trait]
pub trait ConnectionManager {
    async fn handle_incoming_connection(&self, conn: Connection) -> Result<()>;

    async fn get_or_open_connection(
        &self,
        remote_addr: impl Into<NodeAddr> + Clone + Send,
        alpn: &[u8],
    ) -> Result<Connection>;
}
