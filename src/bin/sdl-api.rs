use std::sync::Arc;
use std::time::Duration;

use actix_web::{web, App, HttpServer};
use sdl::api::types::ApiState;

const DEFAULT_MAX_STREAMS: usize = 4;
const HTTP_TIMEOUT: Duration = Duration::from_secs(30);

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let bind_address = std::env::var("SDL_API_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_owned());
    let max_streams = std::env::var("SDL_API_MAX_STREAMS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_MAX_STREAMS);
    let client = reqwest::ClientBuilder::new()
        .timeout(HTTP_TIMEOUT)
        .build()
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))?;
    let state = ApiState {
        download_semaphore: Arc::new(tokio::sync::Semaphore::new(max_streams)),
        client,
    };

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(state.clone()))
            .configure(sdl::api::routes::configure)
    })
    .bind(bind_address)?
    .run()
    .await
}
