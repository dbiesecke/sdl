use std::sync::Arc;
use std::time::Duration;

use actix_web::{web, App, HttpServer};
use sdl::api::{self, ApiState};
use tokio::sync::Semaphore;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();

    let bind = std::env::var("SDL_API_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_owned());
    let max_streams = std::env::var("SDL_API_MAX_STREAMS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(2);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .user_agent(concat!("sdl-api/", env!("CARGO_PKG_VERSION")))
        .build()
        .expect("failed to create reqwest client");

    let state = ApiState {
        client,
        download_semaphore: Arc::new(Semaphore::new(max_streams)),
        extraction_timeout: Duration::from_secs(120),
        permit_timeout: Duration::from_secs(5),
    };

    log::info!("starting sdl-api on {bind} with max_streams={max_streams}");
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(state.clone()))
            .configure(api::configure)
    })
    .bind(bind)?
    .run()
    .await
}
