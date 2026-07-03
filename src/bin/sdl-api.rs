use actix_web::{App, HttpServer};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let bind_address = std::env::var("SDL_API_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_owned());

    HttpServer::new(|| App::new().configure(sdl::api::routes::configure))
        .bind(bind_address)?
        .run()
        .await
}
