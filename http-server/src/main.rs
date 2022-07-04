mod routing;

use actix_web::{App, middleware, HttpServer};
use crate::routing::index;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "actix_web=info");
    // env_logger::init();

    HttpServer::new(|| {
        App::new()
            .wrap(middleware::Logger::default())
            .service(index)
    })
        .bind(("localhost", 5000))?
        .run()
        .await

}
