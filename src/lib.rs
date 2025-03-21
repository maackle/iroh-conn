pub mod basic;
mod manager;

#[cfg(feature = "modeling")]
pub mod event;

#[cfg(feature = "testing")]
pub mod testing;

pub use manager::ConnectionManager;

pub(crate) type Alpn = Vec<u8>;
