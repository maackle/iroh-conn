use crate::Alpn;
use anyhow::Result;
use futures::future::BoxFuture;
use iroh::{
    Endpoint, NodeId,
    endpoint::{ConnectOptions, Connection, TransportConfig, VarInt},
};
use n0_future::FutureExt;

pub trait ConnectionHandler: Send + Sync + 'static {
    fn handle(&self, conn: Connection) -> BoxFuture<'static, Result<()>>;

    fn open(
        &self,
        endpoint: Endpoint,
        remote_node_id: NodeId,
        alpn: Alpn,
    ) -> BoxFuture<'static, Result<Connection>> {
        async move {
            // TODO: 5s is too low
            let opts = ConnectOptions::new().with_transport_config({
                let mut cfg = TransportConfig::default();
                cfg.max_idle_timeout(Some(VarInt::from_u32(10_000).into()));
                cfg.into()
            });
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
