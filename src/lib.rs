mod manager;

pub mod event;

#[cfg(feature = "testing")]
pub mod testing;

pub use manager::{ConnectionHandler, ConnectionManager};
