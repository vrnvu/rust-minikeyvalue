use std::{
    collections::{HashMap, HashSet},
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
    let lock_keys = Arc::new(Mutex::new(HashSet::<String>::new()));
    let lock_keys = warp::any().map(move || lock_keys.clone());

    let put_record = warp::put()
        .and(lock_keys.clone())
        .and(warp::header::optional::<u64>("content-length"))
        .and(warp::path::param())
        .and(warp::body::bytes())
        .and(warp::path::end())
        .map(handle_put_record);

    let get_record = warp::get()
        .and(warp::path::param())
        .and(warp::path::end())
        .map(handle_get_record);

    let api = put_record.or(get_record);

    warp::serve(api).run(([127, 0, 0, 1], port)).await;

    Ok(())
}

fn handle_put_record(
    lock_keys: Arc<Mutex<HashSet<String>>>,
    content_length: Option<u64>,
    key: String,
    value: Bytes,
) -> impl warp::Reply {
    debug!("put_record: key: {}, value: {:?}", key, value);
    if content_length.is_none() {
        return warp::http::Response::builder()
            .status(warp::http::StatusCode::LENGTH_REQUIRED)
            .body("Content-Length is required".to_string());
    }

    let mut lock_keys = match lock_keys.lock() {
        Ok(lock_keys) => lock_keys,
        Err(e) => {
            return warp::http::Response::builder()
                .status(warp::http::StatusCode::INTERNAL_SERVER_ERROR)
                .body(e.to_string())
        }
    };

    if lock_keys.contains(&key) {
        return warp::http::Response::builder()
            .status(warp::http::StatusCode::CONFLICT)
            .body(String::new());
    }

    lock_keys.insert(key.clone());

    // TODO: write to leveldb and write to volumes

    lock_keys.remove(&key);

    warp::http::Response::builder()
        .status(warp::http::StatusCode::CREATED)
        .body(key)
}

fn handle_get_record(key: String) -> impl warp::Reply {
    debug!("get_record: key: {}", key);
    warp::http::Response::builder()
        .status(warp::http::StatusCode::FOUND)
        .body("TODO redirect to nginx volume server".to_string())
}
