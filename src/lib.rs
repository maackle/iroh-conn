mod handler;
pub use handler::ConnectionHandler;

mod manager;

pub mod matheus;
pub mod rpc_manager;

#[cfg(feature = "modeling")]
pub mod event;

#[cfg(feature = "testing")]
pub mod testing;

pub use manager::ConnectionManager;

pub(crate) type Alpn = Vec<u8>;
