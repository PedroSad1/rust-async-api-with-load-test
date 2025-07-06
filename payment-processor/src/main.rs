use actix_web::{App, HttpResponse, HttpServer, Responder, post, web};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Request {
    correlation_id: String,
    amount: f64,
}

#[post("/payments")]
async fn post_payments(req_body: web::Json<Request>) -> impl Responder {
    let payload = req_body.into_inner();

    return HttpResponse::Ok().json(payload);
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("Server running at http://0.0.0.0:8081");

    HttpServer::new(|| App::new().service(post_payments))
        .bind("0.0.0.0:8081")?
        .run()
        .await
}
