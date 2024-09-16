use std::{
    collections::HashSet,
    path::Path,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use clap::Parser;
use log::{debug, error, info};
use warp::Filter;

mod record;

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
    let leveldb_path = Path::new(&cli.ldb);
    let verify_checksums = cli.md5sum;

    let leveldb = {
        let leveldb = record::LevelDb::new(leveldb_path, verify_checksums)?;
        let leveldb = Arc::new(Mutex::new(leveldb));
        warp::any().map(move || leveldb.clone())
    };

    let lock_keys = {
        let lock_keys = HashSet::<String>::new();
        let lock_keys = Arc::new(Mutex::new(lock_keys));
        warp::any().map(move || lock_keys.clone())
    };

    let put_record = warp::put()
        .and(lock_keys.clone())
        .and(leveldb.clone())
        .and(warp::header::optional::<u64>("content-length"))
        .and(warp::path::param::<String>())
        .and(warp::body::bytes())
        .and(warp::path::end())
        .and_then(handle_put_record);

    let get_record = warp::get()
        .and(warp::path::param::<String>())
        .and(warp::path::end())
        .and_then(handle_get_record);

    let api = put_record.or(get_record).recover(handle_rejection);

    warp::serve(api).run(([127, 0, 0, 1], port)).await;

    Ok(())
}

pub async fn handle_rejection(err: warp::Rejection) -> Result<impl warp::Reply, warp::Rejection> {
    let message = String::new();
    let code = {
        if err.is_not_found() {
            warp::http::StatusCode::NOT_FOUND
        } else if let Some(_) = err.find::<warp::reject::MethodNotAllowed>() {
            warp::http::StatusCode::METHOD_NOT_ALLOWED
        } else {
            warp::http::StatusCode::INTERNAL_SERVER_ERROR
        }
    };

    Ok(warp::http::Response::builder().status(code).body(message))
}

async fn handle_put_record(
    lock_keys: Arc<Mutex<HashSet<String>>>,
    leveldb: Arc<Mutex<record::LevelDb>>,
    content_length: Option<u64>,
    key: String,
    value: Bytes,
) -> Result<impl warp::Reply, warp::Rejection> {
    info!("put_record: key: {}, value: {:?}", key, value);
    if content_length.is_none() {
        debug!("put_record: content_length is none for key: {}", key);
        return Ok(warp::http::Response::builder()
            .status(warp::http::StatusCode::LENGTH_REQUIRED)
            .body("Content-Length is required".to_string()));
    }

    {
        let mut lock_keys = match lock_keys.lock() {
            Ok(lock_keys) => lock_keys,
            Err(e) => {
                error!("put_record: failed to lock lock_keys: {}", e);
                return Ok(warp::http::Response::builder()
                    .status(warp::http::StatusCode::INTERNAL_SERVER_ERROR)
                    .body(e.to_string()));
            }
        };

        if lock_keys.contains(&key) {
            return Ok(warp::http::Response::builder()
                .status(warp::http::StatusCode::CONFLICT)
                .body(String::new()));
        }

        lock_keys.insert(key.clone());
    }

    // TODO: write to leveldb and write to volumes
    let leveldb = match leveldb.lock() {
        Ok(leveldb) => leveldb,
        Err(e) => {
            error!("put_record: failed to lock leveldb: {}", e);
            return Ok(warp::http::Response::builder()
                .status(warp::http::StatusCode::INTERNAL_SERVER_ERROR)
                .body(e.to_string()));
        }
    };

    let record: record::Record = match leveldb.get_record_or_default(&key) {
        Ok(record) => record,
        Err(e) => {
            error!(
                "put_record: failed to get record {} from leveldb: {}",
                key, e
            );
            return Ok(warp::http::Response::builder()
                .status(warp::http::StatusCode::INTERNAL_SERVER_ERROR)
                .body(e.to_string()));
        }
    };

    if let record::Deleted::No = record.deleted() {
        return Ok(warp::http::Response::builder()
            .status(warp::http::StatusCode::CONFLICT)
            .body("Forbidden to overwrite with PUT".to_string()));
    }

    // TODO partNumber
    // TODO write_to_replicas(key, value, content_length.unwrap());

    {
        let mut lock_keys = match lock_keys.lock() {
            Ok(lock_keys) => lock_keys,
            Err(e) => {
                error!("put_record: failed to lock lock_keys: {}", e);
                return Ok(warp::http::Response::builder()
                    .status(warp::http::StatusCode::INTERNAL_SERVER_ERROR)
                    .body(e.to_string()));
            }
        };

        lock_keys.remove(&key);
    }

    Ok(warp::http::Response::builder()
        .status(warp::http::StatusCode::CREATED)
        .body(key))
}

async fn handle_get_record(key: String) -> Result<impl warp::Reply, warp::Rejection> {
    info!("get_record: key: {}", key);
    Ok(warp::http::Response::builder()
        .status(warp::http::StatusCode::FOUND)
        .body("TODO redirect to nginx volume server".to_string()))
}
