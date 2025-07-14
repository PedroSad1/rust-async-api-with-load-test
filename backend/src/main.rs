#![allow(non_snake_case)]

use actix_web::middleware::Logger;
use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use redis::AsyncCommands;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::env;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::Mutex;
use uuid::Uuid;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Deserialize, Serialize, Clone)]
struct Request {
    correlationId: Uuid,
    amount: f64,
}

struct QueueState {
    last_written_id: AtomicU64,
    next_seq_id: AtomicU64,
}

#[derive(Clone)]
struct ApiHealthState {
    is_healthy: Arc<AtomicBool>,
    last_checked: Arc<AtomicU64>,
    min_response_time: Arc<AtomicU64>,
    url: String,
    health_url: String,
}

impl ApiHealthState {
    fn new(url: &str, health_url: &str) -> Self {
        Self {
            is_healthy: Arc::new(AtomicBool::new(true)),
            last_checked: Arc::new(AtomicU64::new(0)),
            min_response_time: Arc::new(AtomicU64::new(0)),
            url: url.to_string(),
            health_url: health_url.to_string(),
        }
    }

    fn should_check(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        now.saturating_sub(self.last_checked.load(Ordering::SeqCst)) > 5000
    }

    async fn check_health(&self, client: &Client) {
        if self.should_check() {
            match client.get(&self.health_url).send().await {
                Ok(resp) => {
                    if let Ok(body) = resp.json::<serde_json::Value>().await {
                        let min_time = body["minResponseTime"].as_u64().unwrap_or(0);
                        let failing = body["failing"].as_bool().unwrap_or(true);
                        self.is_healthy.store(!failing, Ordering::SeqCst);
                        self.min_response_time.store(min_time, Ordering::SeqCst);
                    }
                }
                Err(_) => {
                    self.is_healthy.store(false, Ordering::SeqCst);
                }
            }
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            self.last_checked.store(now, Ordering::SeqCst);
        }
    }
}

#[post("/payments")]
async fn post_payments(
    req_body: String,
    sender: web::Data<Sender<String>>,
    redis_conn: web::Data<Arc<Mutex<redis::aio::Connection>>>,
) -> impl Responder {
    let sender = sender.clone();
    let redis_conn = redis_conn.clone();

    tokio::spawn(async move {
        if let Err(err) = sender.send(req_body.clone()).await {
            eprintln!("Queue error, pushing to Redis fallback queue: {}", err);
            let mut conn = redis_conn.lock().await;
            let _: redis::RedisResult<()> = conn.rpush("fallback:payments", req_body).await;
        }
    });

    HttpResponse::Accepted().finish()
}

#[get("/payments-summary")]
async fn get_payments_summary(
    redis_conn: web::Data<Arc<Mutex<redis::aio::Connection>>>,
    state: web::Data<Arc<QueueState>>,
) -> impl Responder {
    let expected_id = state.next_seq_id.load(Ordering::SeqCst).saturating_sub(1);

    while state.last_written_id.load(Ordering::SeqCst) < expected_id {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let mut conn = redis_conn.lock().await;
    let values: Vec<String> = match conn.hvals("payments").await {
        Ok(v) => v,
        Err(err) => {
            eprintln!("Redis read error: {}", err);
            return HttpResponse::InternalServerError().body("Failed to fetch payments");
        }
    };

    let payments: Vec<Request> = values
        .into_iter()
        .filter_map(|v| serde_json::from_str(&v).ok())
        .collect();

    HttpResponse::Ok().json(payments)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv::dotenv().ok();

    let default_url = env::var("EXTERNAL_DEFAULT_API_URL").expect("EXTERNAL_DEFAULT_API_URL must be set");
    let default_health = env::var("EXTERNAL_DEFAULT_API_HEALTH_URL").expect("EXTERNAL_DEFAULT_API_HEALTH_URL must be set");
    let fallback_url = env::var("EXTERNAL_FALLBACK_API_URL").expect("EXTERNAL_FALLBACK_API_URL must be set");
    let fallback_health = env::var("EXTERNAL_FALLBACK_API_HEALTH_URL").expect("EXTERNAL_FALLBACK_API_HEALTH_URL must be set");
    let redis_url: String = env::var("REDIS_URL").expect("REDIS_URL must be set");

    let client = Arc::new(Client::new());
    let redis_client = redis::Client::open(redis_url).expect("Invalid Redis URL");
    let redis_conn = Arc::new(Mutex::new(redis_client.get_async_connection().await.expect("Failed to connect to Redis")));

    let default_api = ApiHealthState::new(&default_url, &default_health);
    let fallback_api = ApiHealthState::new(&fallback_url, &fallback_health);

    let (tx_stage1, mut rx_stage1) = mpsc::channel::<String>(100);
    let (tx_stage2, mut rx_stage2) = mpsc::channel::<(u64, Request)>(100);
    let state = Arc::new(QueueState {
        last_written_id: AtomicU64::new(0),
        next_seq_id: AtomicU64::new(0),
    });

    let client_clone = client.clone();
    let default_api_clone = default_api.clone();
    let fallback_api_clone = fallback_api.clone();
    let state_clone_for_stage1 = state.clone();

    tokio::spawn(async move {
        while let Some(json_str) = rx_stage1.recv().await {
            let payload: Request = match serde_json::from_str(&json_str) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("Invalid JSON payload: {e}");
                    continue;
                }
            };

            let id = state_clone_for_stage1.next_seq_id.fetch_add(1, Ordering::SeqCst);

            default_api_clone.check_health(&client_clone).await;
            fallback_api_clone.check_health(&client_clone).await;

            let use_fallback = !default_api_clone.is_healthy.load(Ordering::SeqCst)
                || default_api_clone.min_response_time.load(Ordering::SeqCst) > 1000;

            let primary_url = if use_fallback && fallback_api_clone.is_healthy.load(Ordering::SeqCst) {
                &fallback_api_clone.url
            } else {
                &default_api_clone.url
            };

            let res = client_clone.post(primary_url).json(&payload).send().await;

            if res.is_ok() {
                let _ = tx_stage2.send((id, payload)).await;
            } else {
                eprintln!("Both APIs failed or unavailable for ID {}", id);
            }
        }
    });

    let redis_conn_clone = redis_conn.clone();
    let state_clone = state.clone();
    tokio::spawn(async move {
        while let Some((id, request)) = rx_stage2.recv().await {
            let mut conn = redis_conn_clone.lock().await;
            let _: () = conn.hset(
                "payments",
                request.correlationId.to_string(),
                serde_json::to_string(&request).unwrap(),
            ).await.unwrap();

            state_clone.last_written_id.store(id, Ordering::SeqCst);
        }
    });

    let redis_conn_clone_for_fallback = redis_conn.clone();
    let tx_stage1_clone = tx_stage1.clone();
    tokio::spawn(async move {
        loop {
            let mut conn = redis_conn_clone_for_fallback.lock().await;
            let fallback_payload: Option<String> = conn.lpop("fallback:payments", None).await.ok().flatten();
            drop(conn);

            if let Some(json_str) = fallback_payload {
                if let Err(e) = tx_stage1_clone.send(json_str.clone()).await {
                    eprintln!("Failed to requeue fallback payload: {e}");
                    let mut conn = redis_conn_clone_for_fallback.lock().await;
                    let _: redis::RedisResult<()> = conn.rpush("fallback:payments", json_str).await;
                }
            } else {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    });

    println!("Server running at http://0.0.0.0:8080");

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(redis_conn.clone()))
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
