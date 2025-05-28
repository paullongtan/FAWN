use clap::Parser;

mod storage;
mod client;
mod client_factory;

use client_factory::new_client;
use storage::Storage;
use std::path::Path;
use rustyline::{Editor};
use rand::Rng;
use std::fs;
use std::fs::File;

use std::io::{Write, BufWriter};
#[derive(Parser, Debug)]
#[clap(name = "client")]
struct Options {
    /// Address for the backend server to listen on
    #[clap(short, long, default_value = "127.0.0.1:8080")]
    address: String,

    /// Addresses of frontend servers
    #[clap(short, long, value_delimiter = ',', default_values = &["127.0.0.1:30010", "127.0.0.1:30011"])]
    frontends: Vec<String>,
}

fn generate_file(path: &Path, num_bytes: usize) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;  // Create directories recursively if needed
    }

    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    let mut rng = rand::thread_rng();

    let buffer_size = 8192;
    let mut remaining = num_bytes;
    let mut buffer = vec![0u8; buffer_size];

    while remaining > 0 {
        let write_size = remaining.min(buffer_size);
        for byte in &mut buffer[..write_size] {
            *byte = rng.r#gen();
        }
        writer.write_all(&buffer[..write_size])?;
        remaining -= write_size;
    }
    writer.flush()?;
    Ok(())
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = Options::parse();
    let client = new_client(&options.address, &options.frontends).await?;

    let mut rl = Editor::<()>::new()?;
    println!("Welcome to FAWN CLI. Commands: put <k> <v>, get <k>, delete <k>, quit");

    loop {
        let line = rl.readline("fawn> ");
        match line {
            Ok(input) => {
                let tokens: Vec<&str> = input.trim().split_whitespace().collect();
                if tokens.is_empty() {
                    continue;
                }

                match tokens[0].to_lowercase().as_str() {
                    "put" if tokens.len() == 3 => {
                        let key = tokens[1];
                        let filename = tokens[2];
                        let path = std::path::Path::new("test/put").join(filename);
                        match client.put(key, &path).await {
                            Ok(_) => println!("Successfully uploaded '{}'", tokens[2]),
                            Err(e) => eprintln!("Failed to upload '{}': {}", tokens[2], e),
                        }
                    }
                    "get" if tokens.len() == 2 => {
                        match client.get(tokens[1]).await {
                            Ok(_) => println!("Successfully fetched '{}'", tokens[1]),
                            Err(e) => eprintln!("Failed to fetch '{}': {}", tokens[1], e),
                        }
                    }
                    "delete" if tokens.len() == 2 => {
                        match client.delete(tokens[1]).await {
                            Ok(_) => println!("Successfully deleted '{}'", tokens[1]),
                            Err(e) => eprintln!("Failed to delete '{}': {}", tokens[1], e),
                        }
                    }
                    // automatically puts into test_data directory
                    "generate" if tokens.len() == 3 => {
                        let filename = tokens[1];
                        let path = std::path::Path::new("test/put").join(filename);
                        match tokens[2].parse::<usize>() {
                            Ok(num_bytes) => {
                                match generate_file(&path, num_bytes) {
                                    Ok(_) => println!("Generated {} bytes at {:?}", num_bytes, path),
                                    Err(e) => eprintln!("Failed to generate file: {}", e),
                                }
                            }
                            Err(_) => println!("Invalid number of bytes: {}", tokens[2]),
                        }
                    }
                    "quit" | "exit" => break,
                    _ => println!("Invalid command. Try: put <key> <file_path>, get <key>, delete <key>, quit"),
                }
            }
            Err(_) => break,
        }
    }
    Ok(())
}
