use std::{collections::HashSet, net::IpAddr, path::Path, str::FromStr, sync::Arc};

use bytes::Bytes;
use clap::Parser;
use log::{debug, error, info};
use rand::{seq::SliceRandom, SeedableRng};
use tokio::sync::Mutex;
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
    leveldb_path: String,

    /// Calculate and store the MD5 checksum of values
    #[clap(long, default_value = "true")]
    hash_md5_checksum: bool,

    /// Sets the volumes
    #[clap(long, value_delimiter = ',')]
    volumes: Vec<String>,

    /// Sets the number of replicas
    #[clap(long, default_value = "3")]
    replicas: usize,

    /// Sets the number of subvolumes
    #[clap(long, default_value = "10")]
    subvolumes: u32,
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
    let leveldb_path = Path::new(&cli.leveldb_path);
    let verify_checksums = cli.hash_md5_checksum;
    let volumes = cli.volumes;
    let replicas = cli.replicas;
    let subvolumes = cli.subvolumes;

    let leveldb = {
        let leveldb = record::LevelDb::new(leveldb_path)?;
        let leveldb = Arc::new(Mutex::new(leveldb));
        warp::any().map(move || leveldb.clone())
    };

    let lock_keys = {
        let lock_keys = HashSet::<String>::new();
        let lock_keys = Arc::new(Mutex::new(lock_keys));
        warp::any().map(move || lock_keys.clone())
    };

    let put_record_context = PutRecordContext {
        volumes: volumes.clone(),
        replicas,
        subvolumes,
        verify_checksums,
    };
    let put_record = warp::put()
        .and(lock_keys.clone())
        .and(leveldb.clone())
        .and(warp::path::param::<String>())
        .and(warp::header::optional::<u64>("content-length"))
        .and(warp::body::bytes())
        .and(warp::any().map(move || put_record_context.clone()))
        .and(warp::path::end())
        .and_then(handle_put_record);

    let get_record = warp::get()
        .and(leveldb.clone())
        .and(warp::path::param::<String>())
        .and(warp::path::end())
        .and(warp::any().map(move || volumes.clone()))
        .and(warp::any().map(move || replicas))
        .and(warp::any().map(move || subvolumes))
        .and_then(handle_get_record);

    let api = put_record.or(get_record).recover(handle_rejection);

    // Listen ipv4 and ipv6
    let addr = IpAddr::from_str("::0").unwrap();
    warp::serve(api).run((addr, port)).await;

    Ok(())
}

pub async fn handle_rejection(err: warp::Rejection) -> Result<impl warp::Reply, warp::Rejection> {
    let message = String::new();
    let code = {
        if err.is_not_found() {
            warp::http::StatusCode::NOT_FOUND
        } else if err.find::<warp::reject::MethodNotAllowed>().is_some() {
            warp::http::StatusCode::METHOD_NOT_ALLOWED
        } else {
            warp::http::StatusCode::INTERNAL_SERVER_ERROR
        }
    };

    Ok(warp::http::Response::builder().status(code).body(message))
}

#[derive(Debug, Clone)]
struct PutRecordContext {
    volumes: Vec<String>,
    replicas: usize,
    subvolumes: u32,
    verify_checksums: bool,
}

async fn handle_put_record(
    lock_keys: Arc<Mutex<HashSet<String>>>,
    leveldb: Arc<Mutex<record::LevelDb>>,
    key: String,
    content_length: Option<u64>,
    value: Bytes,
    put_record_context: PutRecordContext,
) -> Result<impl warp::Reply, warp::Rejection> {
    let PutRecordContext {
        volumes,
        replicas,
        subvolumes,
        verify_checksums,
    } = put_record_context;

    info!("put_record: key: {}, value: {:?}", key, value);
    if content_length.is_none() {
        debug!("put_record: content_length is none for key: {}", key);
        return Ok(warp::http::Response::builder()
            .status(warp::http::StatusCode::LENGTH_REQUIRED)
            .body("Content-Length is required".to_string()));
    }

    if key.is_empty() {
        return Ok(warp::http::Response::builder()
            .status(warp::http::StatusCode::LENGTH_REQUIRED)
            .body("Key cannot be empty".to_string()));
    }

    // TODO: handle mutex better for lock_keys and leveldb
    let mut lock_keys = lock_keys.lock().await;

    if lock_keys.contains(&key) {
        return Ok(warp::http::Response::builder()
            .status(warp::http::StatusCode::CONFLICT)
            .body(String::new()));
    }

    lock_keys.insert(key.clone());

    let leveldb = leveldb.lock().await;
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
    let replicas_volumes = record::get_volume(&key, volumes, replicas, subvolumes);
    let record = record::Record::new(
        record::Deleted::Init,
        String::new(),
        replicas_volumes.clone(),
    );
    match leveldb.put_record(&key, record) {
        Ok(_) => (),
        Err(e) => {
            error!("put_record: failed to put record {} in leveldb: {}", key, e);
            return Ok(warp::http::Response::builder()
                .status(warp::http::StatusCode::INTERNAL_SERVER_ERROR)
                .body(e.to_string()));
        }
    }

    for volume in replicas_volumes.clone() {
        let remote_replica_volume_path = record::get_remote_path(&key);
        let remote_url = format!("http://{}{}", volume, remote_replica_volume_path);
        // TODO is this value Bytes an efficient buffer?
        info!("put_record key: {} remote_url: {}", key, remote_url);
        match remote_put(remote_url, &value).await {
            Ok(_) => (),
            Err(e) => {
                error!(
                    "put_record: failed to put record {} in remote replica volume {} with path {}: {}",
                    key, volume, remote_replica_volume_path, e
                );

                // https://github.com/geohot/minikeyvalue/pull/48/files
                let record =
                    record::Record::new(record::Deleted::Soft, String::new(), replicas_volumes);
                match leveldb.put_record(&key, record) {
                    Ok(_) => (),
                    Err(e) => {
                        error!("put_record: failed to put record {} in leveldb: {}", key, e);
                        return Ok(warp::http::Response::builder()
                            .status(warp::http::StatusCode::INTERNAL_SERVER_ERROR)
                            .body(e.to_string()));
                    }
                }

                return Ok(warp::http::Response::builder()
                    .status(warp::http::StatusCode::INTERNAL_SERVER_ERROR)
                    .body(e.to_string()));
            }
        }
    }

    let value_md5_hash = if verify_checksums {
        let hash = md5::compute(value);
        format!("{:x}", hash)
    } else {
        String::new()
    };

    let record = record::Record::new(record::Deleted::No, value_md5_hash, replicas_volumes);
    match leveldb.put_record(&key, record) {
        Ok(_) => (),
        Err(e) => {
            error!(
                "put_record: failed to put record with value_md5_hash {} in leveldb: {}",
                key, e
            );
            return Ok(warp::http::Response::builder()
                .status(warp::http::StatusCode::INTERNAL_SERVER_ERROR)
                .body(e.to_string()));
        }
    }

    lock_keys.remove(&key);

    Ok(warp::http::Response::builder()
        .status(warp::http::StatusCode::CREATED)
        .body(String::new()))
}

async fn remote_put(remote_url: String, value: &bytes::Bytes) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let res = client
        .put(remote_url.clone())
        .body(value.clone())
        .send()
        .await?;
    if res.status().is_success() {
        if res.status().as_u16() != warp::http::StatusCode::CREATED.as_u16()
            && res.status().as_u16() != warp::http::StatusCode::NO_CONTENT.as_u16()
        {
            return Err(anyhow::anyhow!(
                "remote_put: invalid status code: {} for url: {}",
                res.status(),
                remote_url
            ));
        }
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "remote_put: failed to put value at {}: {}",
            remote_url,
            res.status()
        ))
    }
}

async fn handle_get_record(
    leveldb: Arc<Mutex<record::LevelDb>>,
    key: String,
    volumes: Vec<String>,
    replicas: usize,
    subvolumes: u32,
) -> Result<impl warp::Reply, warp::Rejection> {
    info!("get_record: key: {}", key);

    if key.is_empty() {
        return Ok(warp::http::Response::builder()
            .status(warp::http::StatusCode::LENGTH_REQUIRED)
            .body("Key cannot be empty".to_string()));
    }

    let leveldb = leveldb.lock().await;
    let record = match leveldb.get_record_or_default(&key) {
        Ok(record) => record,
        Err(e) => {
            error!(
                "get_record: failed to get record {} from leveldb: {}",
                key, e
            );
            return Ok(warp::http::Response::builder()
                .status(warp::http::StatusCode::INTERNAL_SERVER_ERROR)
                .body(e.to_string()));
        }
    };

    // TODO fallbacks
    if record.deleted() != record::Deleted::No {
        return Ok(warp::http::Response::builder()
            .status(warp::http::StatusCode::NOT_FOUND)
            .header("Content-Md5", record.hash())
            .header("Content-Length", "0")
            .body(String::new()));
    }

    let replicas_volumes = record::get_volume(&key, volumes, replicas, subvolumes);
    let needs_rebalance_header = if needs_rebalance(&key, &replicas_volumes, record.read_volumes())
    {
        "unbalanced"
    } else {
        "balanced"
    };

    let remote_url: Option<String> = {
        let mut found_remote_url = None;
        let mut rnd = rand::rngs::StdRng::from_entropy();
        for volume in record.read_volumes().choose(&mut rnd).into_iter() {
            let remote_replica_volume_path = record::get_remote_path(&key);
            let remote_url = format!("http://{}{}", volume, remote_replica_volume_path);
            if let Ok(()) = remote_head(&remote_url).await {
                found_remote_url = Some(remote_url);
                break;
            }
        }
        found_remote_url
    };

    return match remote_url {
        Some(remote_url) => {
            info!("get_record: key: {} from remote_url: {}", key, remote_url);
            Ok(warp::http::Response::builder()
                .header("Key-Volumes", record.read_volumes().join(","))
                .header("Key-Balance", needs_rebalance_header)
                .header("Content-Md5", record.hash())
                .header("Content-Length", "0")
                .header("Location", remote_url)
                .status(warp::http::StatusCode::FOUND)
                .body(String::new()))
        }
        None => {
            info!("get_record: key: {} not found", key);
            Ok(warp::http::Response::builder()
                .header("Key-Volumes", record.read_volumes().join(","))
                .header("Key-Balance", needs_rebalance_header)
                .header("Content-Length", "0")
                .status(warp::http::StatusCode::NOT_FOUND)
                .body(String::new()))
        }
    };
}

fn needs_rebalance(key: &str, replicas_volumes: &[String], record_read_volumes: &[String]) -> bool {
    if replicas_volumes.len() != record_read_volumes.len() {
        error!("get_record: key: {} needs rebalance", key);
        return true;
    }

    for i in 0..replicas_volumes.len() {
        if replicas_volumes[i] != record_read_volumes[i] {
            error!("get_record: key: {} needs rebalance", key);
            return true;
        }
    }

    false
}

async fn remote_head(remote_url: &str) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let res = client.head(remote_url).send().await?;
    if res.status().is_success() {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "remote_head: failed to head {}: {}",
            remote_url,
            res.status()
        ))
    }
}
