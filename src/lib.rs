mod manager;

#[cfg(feature = "testing")]
pub mod testing;

pub use manager::{ConnectionHandler, ConnectionManager};
