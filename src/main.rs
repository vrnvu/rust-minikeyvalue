use std::{collections::HashSet, net::IpAddr, path::Path, str::FromStr, sync::Arc};

use bytes::Bytes;
use clap::Parser;
use log::{debug, error};
use rand::{seq::SliceRandom, SeedableRng};
use tokio::sync::{RwLock, RwLockWriteGuard};
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
        let leveldb = Arc::new(leveldb);
        warp::any().map(move || leveldb.clone())
    };

    let lock_keys = {
        let lock_keys = RwLock::new(HashSet::<String>::new());
        let lock_keys = Arc::new(lock_keys);
        warp::any().map(move || lock_keys.clone())
    };

    let client = reqwest::Client::new();
    let put_record_context = PutRecordContext {
        client: client.clone(),
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

    let volumes_clone = volumes.clone();
    let client_clone = client.clone();
    let get_record = warp::get()
        .and(warp::any().map(move || client_clone.clone()))
        .and(leveldb.clone())
        .and(warp::path::param::<String>())
        .and(warp::path::end())
        .and(warp::any().map(move || volumes_clone.clone()))
        .and(warp::any().map(move || replicas))
        .and(warp::any().map(move || subvolumes))
        .and_then(handle_get_record);

    let head_record = warp::head()
        .and(warp::any().map(move || client.clone()))
        .and(leveldb.clone())
        .and(warp::path::param::<String>())
        .and(warp::path::end())
        .and(warp::any().map(move || volumes.clone()))
        .and(warp::any().map(move || replicas))
        .and(warp::any().map(move || subvolumes))
        .and_then(handle_get_record);

    let delete_record = warp::delete()
        .and(leveldb.clone())
        .and(lock_keys.clone())
        .and(warp::path::param::<String>())
        .and(warp::path::end())
        .and_then(handle_delete_record);

    let api = put_record
        .or(get_record)
        .or(head_record)
        .or(delete_record)
        .recover(handle_rejection);

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
    client: reqwest::Client,
    volumes: Vec<String>,
    replicas: usize,
    subvolumes: u32,
    verify_checksums: bool,
}

struct LevelDbKeyGuard<'a> {
    guard: RwLockWriteGuard<'a, HashSet<String>>,
    key: String,
}

impl<'a> LevelDbKeyGuard<'a> {
    async fn lock(lock_keys: &'a RwLock<HashSet<String>>, key: String) -> Self {
        let guard = lock_keys.write().await;
        Self { guard, key }
    }
}

impl<'a> Drop for LevelDbKeyGuard<'a> {
    fn drop(&mut self) {
        self.guard.remove(&self.key);
    }
}

async fn handle_put_record(
    lock_keys: Arc<RwLock<HashSet<String>>>,
    leveldb: Arc<record::LevelDb>,
    key: String,
    content_length: Option<u64>,
    value: Bytes,
    put_record_context: PutRecordContext,
) -> Result<impl warp::Reply, warp::Rejection> {
    let PutRecordContext {
        client,
        volumes,
        replicas,
        subvolumes,
        verify_checksums,
    } = put_record_context;

    debug!("put_record: key: {}, value: {:?}", key, value);
    if content_length.is_none() || value.is_empty() {
        debug!(
            "put_record: content_length is none or value is empty for key: {}",
            key
        );
        return Ok(warp::http::Response::builder()
            .status(warp::http::StatusCode::LENGTH_REQUIRED)
            .body("Content-Length and data can not be empty".to_string()));
    }

    if lock_keys.read().await.contains(&key) {
        debug!("put_record: key: {} already locked", key);
        return Ok(warp::http::Response::builder()
            .status(warp::http::StatusCode::CONFLICT)
            .body(String::new()));
    }

    let mut lock_keys = LevelDbKeyGuard::lock(&lock_keys, key.clone()).await;
    lock_keys.guard.insert(key.clone());

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

    if let record::Deleted::No = record.deleted() {
        return Ok(warp::http::Response::builder()
            .status(warp::http::StatusCode::CONFLICT)
            .body("Forbidden to overwrite with PUT".to_string()));
    }

    // TODO partNumber
    let replicas_volumes = record::get_volume(&key, volumes, replicas, subvolumes);
    for volume in replicas_volumes.clone() {
        let remote_replica_volume_path = record::get_remote_path(&key);
        let remote_url = format!("http://{}{}", volume, remote_replica_volume_path);
        // TODO is this value Bytes an efficient buffer?
        debug!("put_record key: {} remote_url: {}", key, remote_url);
        match remote_put(&client, remote_url, &value).await {
            Ok(_) => (),
            Err(e) => {
                error!(
                    "put_record: failed to put record {} in remote replica volume {} with path {}: {}",
                    key, volume, remote_replica_volume_path, e
                );

                // In case of error we want to mark the record as Deleted::Soft in the local leveldb
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
        format!("{:x}", md5::compute(value))
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

    Ok(warp::http::Response::builder()
        .status(warp::http::StatusCode::CREATED)
        .body(String::new()))
}

async fn remote_put(
    client: &reqwest::Client,
    remote_url: String,
    value: &bytes::Bytes,
) -> anyhow::Result<()> {
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
    client: reqwest::Client,
    leveldb: Arc<record::LevelDb>,
    key: String,
    volumes: Vec<String>,
    replicas: usize,
    subvolumes: u32,
) -> Result<impl warp::Reply, warp::Rejection> {
    debug!("get_record: key: {}", key);

    let record = {
        match leveldb.get_record_or_default(&key) {
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
        }
    };

    // TODO fallbacks
    if record.deleted() != record::Deleted::No {
        debug!(
            "get_record: key: {} not found, record deleted: {:?}",
            key,
            record.deleted()
        );
        return Ok(warp::http::Response::builder()
            .status(warp::http::StatusCode::NOT_FOUND)
            .header("Content-Md5", record.hash())
            .header("Content-Length", "0")
            .body(String::new()));
    }

    let replicas_volumes = record::get_volume(&key, volumes, replicas, subvolumes);
    let needs_rebalance_header = if needs_rebalance(&replicas_volumes, record.read_volumes()) {
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
            if let Ok(()) = remote_head(&client, &remote_url).await {
                found_remote_url = Some(remote_url);
                break;
            }
        }
        found_remote_url
    };

    return match remote_url {
        Some(remote_url) => {
            debug!("get_record: key: {} from remote_url: {}", key, remote_url);
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
            debug!("get_record: key: {} not found in any volume", key);
            Ok(warp::http::Response::builder()
                .header("Key-Volumes", record.read_volumes().join(","))
                .header("Key-Balance", needs_rebalance_header)
                .header("Content-Length", "0")
                .status(warp::http::StatusCode::GONE)
                .body(String::new()))
        }
    };
}

fn needs_rebalance(replicas_volumes: &[String], record_read_volumes: &[String]) -> bool {
    replicas_volumes.len() != record_read_volumes.len()
}

async fn remote_head(client: &reqwest::Client, remote_url: &str) -> anyhow::Result<()> {
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
async fn handle_delete_record(
    leveldb: Arc<record::LevelDb>,
    lock_keys: Arc<RwLock<HashSet<String>>>,
    key: String,
) -> Result<impl warp::Reply, warp::Rejection> {
    debug!("delete_record: key: {}", key);

    if lock_keys.read().await.contains(&key) {
        debug!("delete_record: key: {} already locked", key);
        return Ok(warp::http::Response::builder()
            .status(warp::http::StatusCode::CONFLICT)
            .body(String::new()));
    }

    let mut lock_keys = LevelDbKeyGuard::lock(&lock_keys, key.clone()).await;
    lock_keys.guard.insert(key.clone());

    let record = match leveldb.get_record_or_default(&key) {
        Ok(record) => record,
        Err(e) => {
            error!(
                "delete_record: failed to get record {} from leveldb: {}",
                key, e
            );
            return Ok(warp::http::Response::builder()
                .status(warp::http::StatusCode::INTERNAL_SERVER_ERROR)
                .body(e.to_string()));
        }
    };

    // TODO unlink and soft delete, for now we assume soft is always deleted
    // This probalby will make some tests fail with link/unlink
    if record.deleted() == record::Deleted::Hard || record.deleted() == record::Deleted::Soft {
        debug!("delete_record: key: {} already deleted", key);
        return Ok(warp::http::Response::builder()
            .status(warp::http::StatusCode::NOT_FOUND)
            .body(String::new()));
    }

    let deleted_record = record::Record::new(
        record::Deleted::Soft,
        record.hash().to_string(),
        record.read_volumes().to_vec(),
    );
    match leveldb.put_record(&key, deleted_record) {
        Ok(_) => (),
        Err(e) => {
            error!(
                "delete_record: failed to put deleted record {} in leveldb: {}",
                key, e
            );
            return Ok(warp::http::Response::builder()
                .status(warp::http::StatusCode::INTERNAL_SERVER_ERROR)
                .body(e.to_string()));
        }
    }

    // TODO unlink

    Ok(warp::http::Response::builder()
        .status(warp::http::StatusCode::NO_CONTENT)
        .body(String::new()))
}
