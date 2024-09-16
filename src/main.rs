use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use clap::Parser;
use log::debug;
use warp::Filter;

/// minikeyvalue cli
#[derive(Parser, Debug)]
#[clap(
    version = "0.1.0",
    author = "Arnau Diaz <arnaudiaz@duck.com>",
    about = "minikeyvalue cli"
)]
struct Cli {
    /// Sets logging to "debug" level, defaults to "info"
    #[clap(short, long, global = true)]
    verbose: bool,

    /// Sets the port to listen on
    #[clap(short, long, default_value = "3000")]
    port: u16,

    /// Sets the path to the leveldb
    #[clap(short, long)]
    ldb: String,

    /// Calculate and store the MD5 checksum of values
    #[clap(short, long, default_value = "true")]
    md5sum: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    if cli.verbose {
        std::env::set_var("RUST_LOG", "debug");
    } else {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    let port = cli.port;
    let ldb_path = cli.ldb;
    let md5sum = cli.md5sum;
    let db = Arc::new(Mutex::new(HashMap::<String, Bytes>::new()));
    let db = warp::any().map(move || db.clone());

    let put_record = warp::put()
        .and(db.clone())
        .and(warp::path::param())
        .and(warp::body::bytes())
        .and(warp::path::end())
        .map(handle_put_record);

    let get_record = warp::get()
        .and(db.clone())
        .and(warp::path::param())
        .and(warp::path::end())
        .map(handle_get_record);

    let api = put_record.or(get_record);

    warp::serve(api).run(([127, 0, 0, 1], port)).await;

    Ok(())
}

fn handle_put_record(
    db: Arc<Mutex<HashMap<String, Bytes>>>,
    key: String,
    value: Bytes,
) -> impl warp::Reply {
    debug!("put_record: key: {}, value: {:?}", key, value);
    let mut db = match db.lock() {
        Ok(db) => db,
        Err(e) => {
            return warp::http::Response::builder()
                .status(warp::http::StatusCode::INTERNAL_SERVER_ERROR)
                .body(e.to_string())
        }
    };
    db.insert(key.clone(), value);

    warp::http::Response::builder()
        .status(warp::http::StatusCode::CREATED)
        .body(key)
}

fn handle_get_record(db: Arc<Mutex<HashMap<String, Bytes>>>, key: String) -> impl warp::Reply {
    debug!("get_record: key: {}", key);
    let db = match db.lock() {
        Ok(db) => db,
        Err(e) => {
            return warp::http::Response::builder()
                .status(warp::http::StatusCode::INTERNAL_SERVER_ERROR)
                .body(e.to_string())
        }
    };

    let _todo_redirect_to_nginx_volume_server = match db.get(&key) {
        Some(value) => value,
        None => {
            return warp::http::Response::builder()
                .status(warp::http::StatusCode::NOT_FOUND)
                .body(format!("Key not found: {}", key))
        }
    };

    warp::http::Response::builder()
        .status(warp::http::StatusCode::FOUND)
        .body("TODO redirect to nginx volume server".to_string())
}
