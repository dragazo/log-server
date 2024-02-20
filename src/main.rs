use std::time::Duration;

use axum::{Router, Json};
use axum::routing::post;
use axum::extract::State;

use serde::Deserialize;
use tokio::time::sleep;
use async_std::fs::OpenOptions;
use async_std::io::{WriteExt, BufWriter};
use async_std::channel::{self, Sender};

const SERVER_ADDR: &str = "0.0.0.0:3498";
const LOG_PATH: &str = "events.log";
const LOG_FLUSH_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Deserialize)]
struct LogPayload {
    install_id: String,
    time: String,
    msg: String,
}
enum LogCommand {
    Log(LogPayload),
    Flush,
}
async fn log(state: State<Sender<LogCommand>>, Json(payload): Json<LogPayload>) {
    state.send(LogCommand::Log(payload)).await.unwrap();
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let (log_sender, log_receiver) = channel::unbounded();
    tokio::spawn(async move {
        let mut log_file = BufWriter::new(OpenOptions::new().append(true).open(LOG_PATH).await.unwrap());
        let mut flush_delta = 0usize;

        while let Ok(command) = log_receiver.recv().await {
            match command {
                LogCommand::Log(payload) => {
                    let content = format!("[{}] {} > {:?}\n", payload.time, payload.install_id, payload.msg);
                    log_file.write_all(content.as_bytes()).await.unwrap();
                    flush_delta += 1;
                }
                LogCommand::Flush => if flush_delta > 0 {
                    log_file.flush().await.unwrap();
                    log::info!("flushed log ({flush_delta} events)");
                    flush_delta = 0;
                }
            }
        }
    });
    let log_sender_clone = log_sender.clone();
    tokio::spawn(async move {
        loop {
            sleep(LOG_FLUSH_INTERVAL).await;
            log_sender_clone.send(LogCommand::Flush).await.unwrap();
        }
    });

    let app = Router::new()
        .route("/log", post(log).with_state(log_sender.clone()));

    let listener = tokio::net::TcpListener::bind(SERVER_ADDR).await.unwrap();
    log::info!("listening at {SERVER_ADDR}");
    axum::serve(listener, app).await.unwrap();
}
