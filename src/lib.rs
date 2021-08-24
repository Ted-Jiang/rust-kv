mod kv;
mod error;

// why need this? because lib need define share code
pub use kv::KvStore;
pub use error::{KvsError, Result};