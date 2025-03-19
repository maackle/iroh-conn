use std::{io::Write, time::Instant};

use iroh::Endpoint;

pub async fn await_fully_connected(endpoints: impl IntoIterator<Item = Endpoint>) {
    let start = Instant::now();
    print!("awaiting fully connected ");
    let endpoints = endpoints.into_iter().collect::<Vec<_>>();
    let n = endpoints.len();
    loop {
        let mut counts = vec![];
        for e in endpoints.iter() {
            counts.push(e.remote_info_iter().count());
        }
        let num_complete = counts.iter().filter(|c| **c == n - 1).count();
        if num_complete == n {
            println!("\n{n} nodes fully connected in {:?}", start.elapsed());
            break;
        } else {
            if num_complete == 0 {
                print!(".");
            } else {
                print!(" {num_complete}");
            }
            std::io::stdout().flush().unwrap();
            tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;
        }
    }
}
