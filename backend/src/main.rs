use actix_web::middleware::Logger;
use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Executor, PgPool};
use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::{self, Sender};
use uuid::Uuid;

#[derive(Deserialize, Serialize, Clone)]
struct Request {
    correlation_id: Uuid,
    amount: f64,
}

#[derive(Serialize, sqlx::FromRow, Clone)]
struct PaymentResponse {
    correlation_id: Uuid,
    amount: f64,
}
struct QueueState {
    last_written_id: AtomicU64,
    next_seq_id: AtomicU64,
}

#[post("/payments")]
async fn post_payments(
    req_body: web::Json<Request>,
    sender: web::Data<Sender<(u64, Request)>>,
    state: web::Data<Arc<QueueState>>,
) -> impl Responder {
    let payload = req_body.into_inner();
    let seq_id = state.next_seq_id.fetch_add(1, Ordering::SeqCst);

    if let Err(err) = sender.send((seq_id, payload)).await {
        eprintln!("Queue error: {}", err);
        return HttpResponse::InternalServerError().body("Failed to enqueue payment");
    }

    HttpResponse::Created().finish()
}

#[get("/payments-summary")]
async fn get_payments_summary(
    db: web::Data<PgPool>,
    state: web::Data<Arc<QueueState>>,
) -> impl Responder {
    let expected_id = state.next_seq_id.load(Ordering::SeqCst).saturating_sub(1);

    while state.last_written_id.load(Ordering::SeqCst) < expected_id {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    match sqlx::query_as::<_, PaymentResponse>("SELECT * FROM payments")
        .fetch_all(db.get_ref())
        .await
    {
        Ok(payments) => HttpResponse::Ok().json(payments),
        Err(err) => {
            eprintln!("DB read error: {}", err);
            HttpResponse::InternalServerError().body("Failed to fetch payments")
        }
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv::dotenv().ok();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let external_api_url =
        env::var("EXTERNAL_DEFAULT_API_URL").expect("EXTERNAL_DEFAULT_API_URL must be set");

    let client = Arc::new(Client::new());
    let db = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Failed to connect to DB");

    db.execute("CREATE TABLE IF NOT EXISTS payments (correlation_id UUID, amount FLOAT)")
        .await
        .expect("Failed to create table");

    let (tx_stage1, mut rx_stage1) = mpsc::channel::<(u64, Request)>(100);
    let (tx_stage2, mut rx_stage2) = mpsc::channel::<(u64, PaymentResponse)>(100);
    let state = Arc::new(QueueState {
        last_written_id: AtomicU64::new(0),
        next_seq_id: AtomicU64::new(0),
    });

    let client_clone = client.clone();
    tokio::spawn(async move {
        while let Some((id, payload)) = rx_stage1.recv().await {
            let res = client_clone
                .post(&external_api_url)
                .json(&payload)
                .send()
                .await;

            if let Ok(response) = res {
                let json: serde_json::Value = match response.json().await {
                    Ok(val) => val,
                    Err(err) => {
                        eprintln!("Failed to parse response JSON: {err}");
                        continue;
                    }
                };

                let parsed: serde_json::Value =
                    match serde_json::from_str(json.to_string().as_str()) {
                        Ok(val) => val,
                        Err(err) => {
                            eprintln!("Failed to parse inner JSON string: {}", err);
                            continue;
                        }
                    };

                let record = PaymentResponse {
                    correlation_id: Uuid::parse_str(
                        parsed
                            .get("correlation_id")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default(),
                    )
                    .unwrap_or_else(|_| Uuid::nil()),
                    amount: parsed.get("amount").and_then(|v| v.as_f64()).unwrap_or(0.0),
                };

                let _ = tx_stage2.send((id, record)).await;
            } else {
                eprintln!("Failed to call external API for ID {}", id);
            }
        }
    });

    let db_clone = db.clone();
    let state_clone = state.clone();
    tokio::spawn(async move {
        while let Some((id, payment)) = rx_stage2.recv().await {
            let _ = sqlx::query("INSERT INTO payments (correlation_id, amount) VALUES ($1, $2)")
                .bind(payment.correlation_id)
                .bind(payment.amount)
                .execute(&db_clone)
                .await;

            state_clone.last_written_id.store(id, Ordering::SeqCst);
        }
    });

    println!("Server running at http://0.0.0.0:8080");

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(db.clone()))
            .app_data(web::Data::new(client.clone()))
            .app_data(web::Data::new(tx_stage1.clone()))
            .app_data(web::Data::new(state.clone()))
            .wrap(Logger::default())
            .service(post_payments)
            .service(get_payments_summary)
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}
