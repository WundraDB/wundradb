use wundradb_core::Database;
use anyhow::Result;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, error};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "127.0.0.1:3306";
    let listener = TcpListener::bind(addr).await?;
    info!("WundraDB server listening on {}", addr);

    let db = Arc::new(RwLock::new(Database::new("data").await?));

    loop {
        let (stream, addr) = listener.accept().await?;
        info!("New connection from {}", addr);
        let db = db.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_client(stream, db).await {
                error!("Client error: {:?}", e);
            }
        });
    }
}

async fn handle_client(stream: TcpStream, db: Arc<RwLock<Database>>) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    writer.write_all(b"").await?;

    while let Ok(Some(line)) = lines.next_line().await {
        let sql = line.trim();
        if sql.eq_ignore_ascii_case("exit") || sql.eq_ignore_ascii_case("quit") {
            writer.write_all(b"Goodbye!\n").await?;
            break;
        }

        println!("Received: {}", sql);

        let mut db = db.write().await;
        let start = std::time::Instant::now();

        let response = match db.execute_sql(sql).await {
            Ok(result) => format!("{}\nQuery OK Query OK ({:.2?})\n", result, start.elapsed()),
            Err(e) => format!("Error Error: {}\n", e),
        };

        writer.write_all(response.as_bytes()).await?;

    }

    Ok(())
}