#![allow(dead_code)]
//! module containing configuration functions which can aid in configuring
//! and running the tribbler service.

use std::fs;
use std::io::{stdout, Write};
use std::sync::mpsc::Sender;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Receiver;

#[derive(Debug, Serialize, Deserialize)]
pub struct FrontConfig {
    pub fronts: Vec<String>,
    pub this: usize,
}

impl FrontConfig {
    pub fn new(fronts: Vec<String>, this: usize) -> FrontConfig {
        FrontConfig {
            fronts,
            this,
        }
    }

    pub fn from_file(file_path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = fs::read_to_string(file_path)?;
        let config: FrontConfig = serde_json::from_str(&contents)?;
        Ok(config)
    }
}