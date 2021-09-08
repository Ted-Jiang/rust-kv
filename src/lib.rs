mod kv;
mod error;
mod client;
mod server;
mod common;
mod engine;

// why need this? because lib need define share code
pub use client::KvsClient;
pub use engine::{KvStore, KvsEngine, SledKvsEngine};
pub use error::{KvsError, Result};
pub use server::KvsServer;