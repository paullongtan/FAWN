use clap::Parser;


mod storage;
mod client;
mod client_factory;

use client_factory::new_client;
use storage::Storage;
use rustyline::{Editor};

#[derive(Parser, Debug)]
#[clap(name = "kv-client")]
struct Options {
    #[clap(short, long, default_value = "127.0.0.1:7799")]
    address: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = Options::parse();
    let client = new_client(&options.address).await?;

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
                        client.put(tokens[1], tokens[2]).await?;
                    }
                    "get" if tokens.len() == 2 => {
                        match client.get(tokens[1]).await? {
                            Some(val) => println!("Value: {}", val),
                            None => println!("Key not found"),
                        }
                    }
                    "delete" if tokens.len() == 2 => {
                        client.delete(tokens[1]).await?;
                    }
                    "quit" | "exit" => break,
                    _ => println!("Invalid command. Try: put <k> <v>, get <k>, delete <k>, quit"),
                }
            }
            Err(_) => break,
        }
    }

    Ok(())
}
