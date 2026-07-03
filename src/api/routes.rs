use actix_web::web;

use super::handlers;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/api")
            .route("/play", web::get().to(handlers::play_get))
            .route("/play", web::post().to(handlers::play_post))
            .route("/info", web::get().to(handlers::info_get))
            .route("/info", web::post().to(handlers::info_post)),
    );
}
