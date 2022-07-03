
use actix_web::{HttpRequest, get};

#[get("/")]
pub async fn index(req: HttpRequest) -> &'static str {
    println!("REQ: {req:?}");
    "Hello World!"
}
