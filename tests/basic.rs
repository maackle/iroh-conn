#![cfg(feature = "testing")]

use anyhow::Result;
use futures::future::{self, join_all};
use iroh_conn::testing::EchoHandler;
use iroh_conn::testing::{ALPN_ECHO, TestNode, setup_tracing};

const TRACING_DIRECTIVE: &str = "off";
// const TRACING_DIRECTIVE: &str = "basic=info,iroh_conn=info";

#[tokio::test(flavor = "multi_thread")]
async fn test_two_simultaneous() -> Result<()> {
    setup_tracing(TRACING_DIRECTIVE);

    let mapping = Some(Default::default());
    let handler = EchoHandler::new(mapping.clone());
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
    let handler = EchoHandler::new(mapping.clone());
    let [n1, n2, n3] = TestNode::cluster(handler, [ALPN_ECHO.to_vec()], mapping).await?;
    TestNode::rpc_cycle([&n1, &n2, &n3], b"hello").await?;
    TestNode::rpc_cycle([&n1, &n2, &n3], b"hello").await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_interwoven() -> Result<()> {
    setup_tracing(TRACING_DIRECTIVE);

    let mapping = Some(Default::default());
    let handler = EchoHandler::new(mapping.clone());
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
    let handler = EchoHandler::new(mapping.clone());
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
