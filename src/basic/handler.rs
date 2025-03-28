use crate::Alpn;
use anyhow::Result;
use futures::future::BoxFuture;
use iroh::{
    Endpoint, NodeId,
    endpoint::{ConnectOptions, Connection},
};
use n0_future::FutureExt;

/// The ConnectionHandler trait describes how to open connections,
/// and what to do with connections once they are established.
pub trait ConnectionHandler: Send + Sync + 'static {
    /// Do something with a newly established connection.
    ///
    /// This is where your protocol's listening logic goes.
    fn handle(&self, conn: Connection) -> BoxFuture<'static, Result<()>>;

    /// Specify the connection options to use when opening a connection.
    fn connect_options(&self) -> ConnectOptions {
        ConnectOptions::default()
    }

    /// Open a connection to a remote node.
    ///
    /// This may be overridden to provide custom connection logic beyond
    /// specifying the connection options.
    fn open(
        &self,
        endpoint: Endpoint,
        remote_node_id: NodeId,
        alpn: Alpn,
    ) -> BoxFuture<'static, Result<Connection>> {
        let opts = self.connect_options();
        async move {
            // TODO: 5s is too low
            let connecting = endpoint
                .connect_with_opts(remote_node_id, &alpn, opts)
                .await?;
            let conn = connecting.await?;
            // let conn = endpoint.connect(remote_node_id, &alpn).await?;
            tracing::trace!("connection established");
            Ok(conn)
        }
        .boxed()
    }
}

impl ConnectionHandler for () {
    fn handle(&self, _: Connection) -> BoxFuture<'static, Result<()>> {
        async move { Ok(()) }.boxed()
    }
}
