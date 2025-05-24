#![allow(dead_code)]
//! module containing configuration functions which can aid in configuring
//! and running the tribbler service.

use std::fs;
use std::io::{stdout, Write};
use std::sync::mpsc::Sender;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Receiver;


pub struct FrontConfig {
    pub fronts: Vec<String>,
    pub this: usize,
    pub ready: Option<Sender<bool>>,
    pub shutdown: Option<Receiver<()>>,
}

impl FrontConfig {
    pub fn new(fronts: Vec<String>, this: usize) -> FrontConfig {
        todo!()
    }
}