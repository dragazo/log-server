use std::time::Duration;
use std::num::NonZero;

use axum::{Router, Json};
use axum::routing::post;
use axum::extract::{State, DefaultBodyLimit};

use serde::Deserialize;
use serde_json::Value as JsonValue;
use tokio::time::sleep;
use async_std::fs::OpenOptions;
use async_std::io::{WriteExt, BufWriter};
use async_std::channel::{self, Sender};
use clap::Parser;

#[derive(Deserialize)]
struct LogPayload {
    install_id: String,
    session_id: String,
    seq: usize,
    time: String,
    msg: JsonValue,
}
enum LogCommand {
    Log(LogPayload),
    Flush,
}

async fn log(state: State<Sender<LogCommand>>, Json(payload): Json<LogPayload>) {
    state.send(LogCommand::Log(payload)).await.unwrap();
}
async fn flush(state: State<Sender<LogCommand>>) {
    state.send(LogCommand::Flush).await.unwrap();
}

/// Host a simple, general-purpose logging server.
#[derive(Parser)]
struct Args {
    /// Path to store the log file.
    #[arg(short, long, default_value_t = String::from("events.log"))]
    output: String,

    /// Log flush interval in seconds.
    #[arg(short, long, default_value_t = NonZero::new(60).unwrap())]
    flush_interval: NonZero<u64>,

    /// Port to use for the logging server.
    #[arg(short, long, default_value_t = 3498)]
    port: u16,
}

#[tokio::main]
async fn main() {
    simple_logger::SimpleLogger::new().with_level(log::LevelFilter::Info).init().unwrap();

    let args = Args::parse();

    let (log_sender, log_receiver) = channel::unbounded();
    let output = args.output.clone();
    tokio::spawn(async move {
        let mut log_file = BufWriter::new(OpenOptions::new().create(true).append(true).open(&output).await.unwrap());
        let mut flush_delta = 0usize;

        while let Ok(command) = log_receiver.recv().await {
            match command {
                LogCommand::Log(payload) => {
                    let content = format!("[{}] {}::{}::{} > {}\n", payload.time, payload.install_id, payload.session_id, payload.seq, payload.msg);
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
        let t = Duration::from_secs(args.flush_interval.get());
        loop {
            sleep(t).await;
            log_sender_clone.send(LogCommand::Flush).await.unwrap();
        }
    });

    let app = Router::new()
        .route("/log", post(log).with_state(log_sender.clone()))
        .route("/flush", post(flush).with_state(log_sender.clone()))
        .layer(DefaultBodyLimit::max(16 * 1024 * 1024));

    let addr = format!("0.0.0.0:{}", args.port);
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    log::info!("listening at {addr} -- flushing to {} every {} seconds", args.output, args.flush_interval.get());
    axum::serve(listener, app).await.unwrap();
}
