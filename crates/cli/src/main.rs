use anyhow::Result;
use clap::Parser;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use rustyline::Editor;
use std::io::{stdout, Write};

#[derive(Parser, Debug)]
#[command(name = "wundradb-cli")]
struct Args {
    /// Host to connect to
    #[arg(short = 'H', long, default_value = "127.0.0.1")]
    host: String,

    /// Port to connect to
    #[arg(short, long, default_value_t = 3306)]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let addr = format!("{}:{}", args.host, args.port);

    println!("Connecting to WundraDB at {}...", addr);
    let stream = TcpStream::connect(&addr).await?;
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    let mut rl = Editor::<(), _>::new()?;
    loop {
        let readline = rl.readline("wundradb> ");
        match readline {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.eq_ignore_ascii_case("exit") || trimmed.eq_ignore_ascii_case("quit") {
                    writer.write_all(b"exit\n").await?;
                    break;
                }

                writer.write_all(trimmed.as_bytes()).await?;
                writer.write_all(b"\n").await?;

                // Wait for response
                while let Ok(Some(line)) = lines.next_line().await {
                    if line.trim_start().starts_with("Query OK") || line.trim_start().starts_with("Error") {
                        print!("{}", line); // use print! for inline prompt
                        stdout().flush().unwrap(); // ✅ force it to appear immediately
                        break;
                    } else {
                        println!("{}", line); // for normal output
                    }
                }
            }
            Err(_) => {
                println!("Exiting...");
                writer.write_all(b"exit\n").await?;
                break;
            }
        }
    }

    Ok(())
}