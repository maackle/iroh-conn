mod manager;

pub mod handler;
#[cfg(feature = "testing")]
pub mod testing;

pub use handler::ConnectionHandler;
pub use manager::ConnectionManager;

pub(crate) type Alpn = Vec<u8>;
