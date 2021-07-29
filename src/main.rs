use std::net::SocketAddr;
use std::path::Path;

use search_server::start_server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let index_path = Path::new("./data").to_path_buf();
    let addr: SocketAddr = "127.0.0.1:8080".parse()?;
    println!("Running service at http://{}", addr);
    start_server(index_path, addr).await
}
