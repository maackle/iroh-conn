use std::{sync::Arc, time::Duration};

use anyhow::Result;
use futures::{
    FutureExt,
    future::{self, BoxFuture, join_all},
};
use iroh::{
    NodeId,
    endpoint::{ConnectOptions, Connection, TransportConfig, VarInt},
};
use iroh_conn::{
    basic::ConnectionHandler,
    event::{Event, EventMappingShared, EventType},
};
use iroh_conn::{
    event::EventMapping,
    testing::{ALPN_ECHO, TestNode, setup_tracing},
};
use tokio::sync::Mutex;

const TRACING_DIRECTIVE: &str = "off";
// const TRACING_DIRECTIVE: &str = "basic=info,iroh_conn=info";

/// Artificial "processing time" delay for the echo handler
const ECHO_DELAY: Duration = Duration::from_millis(100);

#[tokio::test(flavor = "multi_thread")]
async fn test_two_simultaneous() -> Result<()> {
    setup_tracing(TRACING_DIRECTIVE);

    let mapping = Some(Default::default());
    let handler = EchoHandler(mapping.clone());
    let [n1, n2] = TestNode::cluster(handler, [ALPN_ECHO.to_vec()], mapping).await?;

    println!("\nfirst:\n");
    // First simultaneous call fails
    TestNode::rpc_cycle([&n1, &n2], b"hello").await.unwrap_err();

    println!("\nsecond:\n");
    // Second simultaneous call succeeds
    TestNode::rpc_cycle([&n1, &n2], b"hi").await.unwrap();

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_three_simultaneous() -> Result<()> {
    setup_tracing(TRACING_DIRECTIVE);

    let mapping = Some(Default::default());
    let handler = EchoHandler(mapping.clone());
    let [n1, n2, n3] = TestNode::cluster(handler, [ALPN_ECHO.to_vec()], mapping).await?;
    TestNode::rpc_cycle([&n1, &n2, &n3], b"hello").await?;
    TestNode::rpc_cycle([&n1, &n2, &n3], b"hello").await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_interwoven() -> Result<()> {
    setup_tracing(TRACING_DIRECTIVE);

    let mapping = Some(Default::default());
    let handler = EchoHandler(mapping.clone());
    let mut lock = mapping.as_ref().unwrap().lock().await;

    let [n0, n1, n2] = TestNode::cluster(handler, [ALPN_ECHO.to_vec()], mapping.clone()).await?;

    for n in [&n0, &n1, &n2] {
        let i = lock.nodes.lookup(n.endpoint().node_id()).unwrap();
        println!("{} = {}", i, n.endpoint().node_id().fmt_short());
    }
    drop(lock);

    join_all([
        TestNode::rpc_cycle([&n0, &n1], b"hello"),
        TestNode::rpc_cycle([&n1, &n2], b"hello"),
        TestNode::rpc_cycle([&n2, &n0], b"hello"),
    ])
    .await
    .into_iter()
    .collect::<Result<Vec<_>>>()?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_two_simultaneous_warmed() -> Result<()> {
    setup_tracing(TRACING_DIRECTIVE);

    let mapping = Some(Default::default());
    let handler = EchoHandler(mapping.clone());
    let [n1, n2] = TestNode::cluster(handler, [ALPN_ECHO.to_vec()], mapping).await?;

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
    let calls = [n1.rpc(&n2, b"hello from 1"), n2.rpc(&n1, b"hello from 2")];
    let _result = future::join_all(calls)
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;

    Ok(())
}

#[derive(Clone)]
pub struct EchoHandler(EventMappingShared);

impl ConnectionHandler for EchoHandler {
    fn connect_options(&self) -> ConnectOptions {
        // 1 second idle timeout for these tests
        ConnectOptions::new().with_transport_config({
            let mut cfg = TransportConfig::default();
            cfg.max_idle_timeout(Some(VarInt::from_u32(1_000).into()));
            cfg.into()
        })
    }

    fn handle(&self, node_id: NodeId, conn: Connection) -> BoxFuture<'static, Result<()>> {
        let mapping = self.0.clone();

        async move {
            if conn.alpn() != Some(ALPN_ECHO.to_vec()) {
                anyhow::bail!("expected ALPN {:?}, got {:?}", ALPN_ECHO, conn.alpn());
            }

            while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                if let Some(mapping) = mapping.clone() {
                    let mut lock = mapping.lock().await;
                    let event = Event::new(
                        node_id,
                        EventType::AcceptStream {
                            from: conn.remote_node_id().unwrap(),
                            conn: conn.stable_id(),
                        },
                    );
                    iroh_conn::event::emit_event(event, node_id, &mut lock, None);
                }

                tracing::info!(send = ?send.id(), recv = ?recv.id(), "[accept] BEGIN");

                tokio::time::sleep(ECHO_DELAY).await;

                let buf = recv.read_to_end(10_000).await?;
                tracing::info!("[accept] received msg {}", std::str::from_utf8(&buf)?);

                send.write_all(&buf).await?;
                tracing::info!("[accept] replied with msg {}", std::str::from_utf8(&buf)?);

                send.finish()?;
                tracing::info!("[accept] DONE");

                if let Some(mapping) = mapping.clone() {
                    let mut lock = mapping.lock().await;
                    let event = Event::new(
                        node_id,
                        EventType::EndStream {
                            to: conn.remote_node_id().unwrap(),
                            conn: conn.stable_id(),
                        },
                    );
                    iroh_conn::event::emit_event(event, node_id, &mut lock, None);
                }
            }
            tracing::info!("handler loop for conn {} closing", conn.stable_id());
            Ok(())
        }
        .boxed()
    }
}
