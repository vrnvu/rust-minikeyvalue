use std::{
    collections::HashSet,
    path::Path,
    sync::{Arc, Mutex},
};

use anyhow::Context;
use leveldb::database::Database;
use leveldb::kv::KV;

use bytes::Bytes;
use clap::Parser;
use log::{debug, error};
use serde::{Deserialize, Serialize};
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

#[derive(Debug, Serialize, Deserialize)]
enum Deleted {
    No,
    Soft,
    Hard,
}

#[derive(Debug, Serialize, Deserialize)]
struct Record {
    deleted: Deleted,
    hash: String,
    read_volumes: Vec<String>,
}

impl Record {
    fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| anyhow::anyhow!("Serialization error: {}", e))
    }

    fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        bincode::deserialize(bytes).map_err(|e| anyhow::anyhow!("Deserialization error: {}", e))
    }
}

impl Default for Record {
    fn default() -> Self {
        Self {
            deleted: Deleted::Hard,
            hash: String::new(),
            read_volumes: Vec::new(),
        }
    }
}

impl TryFrom<Option<Vec<u8>>> for Record {
    type Error = anyhow::Error;

    fn try_from(value: Option<Vec<u8>>) -> anyhow::Result<Self> {
        match value {
            Some(data) => Self::from_bytes(&data),
            None => Ok(Record::default()),
        }
    }
}

struct LevelDb {
    leveldb: Database<i32>,
    verify_checksums: bool,
}

impl LevelDb {
    pub fn new(ldb_path: &Path, verify_checksums: bool) -> anyhow::Result<Self> {
        let mut leveldb_options = leveldb::options::Options::new();
        leveldb_options.create_if_missing = true;

        let leveldb = leveldb::database::Database::open(&ldb_path, leveldb_options)
            .with_context(|| format!("Failed to open LevelDB at path: {}", ldb_path.display()))?;

        Ok(Self {
            leveldb,
            verify_checksums,
        })
    }

    pub fn get_record_or_default(&self, key: &str) -> anyhow::Result<Record> {
        let read_options = leveldb::options::ReadOptions::new();
        // TODO make sure i32 is always positive and use only the lower 31 bits of the hash
        let leveldb_key: i32 = (gxhash::gxhash32(key.as_bytes(), 0) & 0x7FFFFFFF) as i32;

        let record = self
            .leveldb
            .get(read_options, leveldb_key)
            .with_context(|| format!("Failed to get key {} from LevelDB", key))?;

        record
            .try_into()
            .with_context(|| format!("Failed to deserialize record for key {}", key))
    }
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
        let leveldb = LevelDb::new(leveldb_path, verify_checksums)?;
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
    leveldb: Arc<Mutex<LevelDb>>,
    content_length: Option<u64>,
    key: String,
    value: Bytes,
) -> Result<impl warp::Reply, warp::Rejection> {
    debug!("put_record: key: {}, value: {:?}", key, value);
    if content_length.is_none() {
        debug!("put_record: content_length is none for key: {}", key);
        return Ok(warp::http::Response::builder()
            .status(warp::http::StatusCode::LENGTH_REQUIRED)
            .body("Content-Length is required".to_string()));
    }

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

    let record = match leveldb.get_record_or_default(&key) {
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

    if let Deleted::No = record.deleted {
        return Ok(warp::http::Response::builder()
            .status(warp::http::StatusCode::CONFLICT)
            .body("Forbidden to overwrite with PUT".to_string()));
    }

    // TODO partNumber
    // TODO write_to_replicas(key, value, content_length.unwrap());

    lock_keys.remove(&key);

    Ok(warp::http::Response::builder()
        .status(warp::http::StatusCode::CREATED)
        .body(key))
}

async fn handle_get_record(key: String) -> Result<impl warp::Reply, warp::Rejection> {
    debug!("get_record: key: {}", key);
    Ok(warp::http::Response::builder()
        .status(warp::http::StatusCode::FOUND)
        .body("TODO redirect to nginx volume server".to_string()))
}
