use actix_web::web;

use crate::api::handlers;

/// Register SDL API routes.
///
/// Handlers delegate to `sdl_wrapper`, which reuses the existing downloader and
/// extractor modules so the HTTP API follows the CLI's hoster support.
pub fn configure(config: &mut web::ServiceConfig) {
    config
        .route("/health", web::get().to(handlers::health))
        .route("/openapi.yaml", web::get().to(handlers::openapi_yaml))
        .service(
            web::scope("/api")
                .route("/play", web::get().to(handlers::get_play))
                .route("/play", web::post().to(handlers::post_play))
                .route("/download", web::get().to(handlers::get_download))
                .route("/download", web::post().to(handlers::post_download))
                .route("/info", web::get().to(handlers::get_info))
                .route("/info", web::post().to(handlers::post_info))
                .route("/info_resolve", web::get().to(handlers::get_info_resolve))
                .route("/info_resolve", web::post().to(handlers::post_info_resolve)),
        );
}
