//! HTTP API for resolving SDL-supported links and streaming resolved video data.
//!
//! The API deliberately reuses the existing downloader and extractor traits so the
//! server supports the same sites and hosters as the CLI.

pub mod error;
pub mod handlers;
pub mod routes;
pub mod sdl_wrapper;
pub mod types;

pub use routes::configure;
pub use types::ApiState;
