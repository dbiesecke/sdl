//! HTTP API surface for SDL.
//!
//! The API modules are intentionally thin wrappers around the existing SDL
//! downloader and extractor stack. They are structured so server routes can
//! reuse `downloaders::find_downloader_for_url`, `InstantiatedDownloader::download`,
//! `AniWorldSerienStream::get_series_info`, and `extractors::extract_video_url*`,
//! keeping API hoster support aligned with the CLI.

pub mod browser;
pub mod error;
pub mod handlers;
pub mod routes;
pub mod sdl_wrapper;
pub mod types;
