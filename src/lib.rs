mod kv;
mod error;
mod client;
mod server;
mod common;
mod engine;
pub mod thread_pool;

// why need this? because lib need define share code
pub use client::KvsClient;
pub use engine::{KvStore, KvsEngine};
pub use error::{KvsError, Result};
pub use server::KvsServer;