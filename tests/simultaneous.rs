use std::time::Duration;

use anyhow::Result;
use futures::{
    FutureExt,
    future::{self, BoxFuture, join_all},
};
use iroh::endpoint::Connection;
use iroh_conn::{
    ConnectionHandler,
    testing::{ALPN_ECHO, TestNode, setup_tracing},
};

/// Artificial "processing time" delay for the echo handler
const ECHO_DELAY: Duration = Duration::from_millis(100);

#[tokio::test(flavor = "multi_thread")]
async fn test_two_simultaneous() -> Result<()> {
    setup_tracing("simultaneous=info,iroh_conn=info");

    let [n1, n2] = TestNode::cluster(EchoHandler, [ALPN_ECHO.to_vec()]).await?;

    // First simultaneous call fails
    TestNode::rpc_cycle([&n1, &n2], b"hello").await.unwrap_err();
    // Second simultaneous call succeeds
    TestNode::rpc_cycle([&n1, &n2], b"hi").await.unwrap();

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_three_simultaneous() -> Result<()> {
    setup_tracing("simultaneous=info,iroh_conn=info");

    let [n1, n2, n3] = TestNode::cluster(EchoHandler, [ALPN_ECHO.to_vec()]).await?;
    TestNode::rpc_cycle([&n1, &n2, &n3], b"hello").await?;
    TestNode::rpc_cycle([&n1, &n2, &n3], b"hello").await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_interweaved() -> Result<()> {
    setup_tracing("simultaneous=info,iroh_conn=info");

    let [n1, n2, n3] = TestNode::cluster(EchoHandler, [ALPN_ECHO.to_vec()]).await?;
    join_all([
        TestNode::rpc_cycle([&n1, &n2], b"hello"),
        TestNode::rpc_cycle([&n2, &n3], b"hello"),
        TestNode::rpc_cycle([&n3, &n1], b"hello"),
    ])
    .await
    .into_iter()
    .collect::<Result<Vec<_>>>()?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_two_simultaneous_warmed() -> Result<()> {
    setup_tracing("simultaneous=info,iroh_conn=info");

    let [n1, n2] = TestNode::cluster(EchoHandler, [ALPN_ECHO.to_vec()]).await?;

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

#[derive(Clone)]
pub struct EchoHandler;

impl ConnectionHandler for EchoHandler {
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
