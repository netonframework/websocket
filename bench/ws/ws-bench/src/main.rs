// websocket SPEC step 5: tokio-tungstenite 0.30 echo server, and a closed-loop load client (one message in flight per
// connection) used against both servers.
//   ws-bench server <addr>
//   ws-bench client <url> <connections> <seconds> <payload-bytes> <threads>
use futures_util::{SinkExt, StreamExt};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::Message;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args[1].as_str() {
        "server" => {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(async {
                let listener = tokio::net::TcpListener::bind(&args[2]).await.unwrap();
                println!("tungstenite echo on {}", args[2]);
                loop {
                    let (s, _) = listener.accept().await.unwrap();
                    let _ = s.set_nodelay(true);
                    tokio::spawn(async move {
                        let Ok(mut ws) = tokio_tungstenite::accept_async(s).await else { return };
                        while let Some(Ok(m)) = ws.next().await {
                            if m.is_text() || m.is_binary() { if ws.send(m).await.is_err() { break; } }
                        }
                    });
                }
            });
        }
        "client" => {
            let url = args[2].clone();
            let conns: usize = args[3].parse().unwrap();
            let secs: u64 = args[4].parse().unwrap();
            let size: usize = args[5].parse().unwrap();
            let threads: usize = args[6].parse().unwrap();
            let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(threads).enable_all().build().unwrap();
            rt.block_on(async move {
                let count = Arc::new(AtomicU64::new(0));
                let lat = Arc::new(std::sync::Mutex::new(Vec::<u32>::new()));
                let deadline = Instant::now() + Duration::from_secs(secs);
                let mut tasks = vec![];
                for _ in 0..conns {
                    let (url, count, lat) = (url.clone(), count.clone(), lat.clone());
                    tasks.push(tokio::spawn(async move {
                        let (mut ws, _) = tokio_tungstenite::connect_async(url.as_str()).await.unwrap();
                        let payload = vec![b'x'; size];
                        let mut local = Vec::with_capacity(1 << 16);
                        while Instant::now() < deadline {
                            let t = Instant::now();
                            ws.send(Message::binary(payload.clone())).await.unwrap();
                            match ws.next().await { Some(Ok(_)) => {}, _ => break }
                            local.push(t.elapsed().as_micros() as u32);
                            count.fetch_add(1, Ordering::Relaxed);
                        }
                        let _ = ws.close(None).await;
                        lat.lock().unwrap().extend(local);
                    }));
                }
                for t in tasks { let _ = t.await; }
                let mut l = lat.lock().unwrap().clone();
                l.sort_unstable();
                let n = count.load(Ordering::Relaxed);
                let p = |q: f64| if l.is_empty() { 0 } else { l[((l.len() as f64 - 1.0) * q) as usize] };
                println!("msgs={} rate={:.0}/s p50={}us p99={}us p999={}us", n, n as f64 / secs as f64, p(0.5), p(0.99), p(0.999));
            });
        }
        _ => panic!("server|client"),
    }
}
