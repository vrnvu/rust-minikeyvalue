use std::{collections::HashSet, path::Path, sync::Arc};

use ::hashring::HashRing;
use clap::Parser;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use log::{debug, error};
use rand::{seq::SliceRandom, SeedableRng};
use reqwest::StatusCode;
use tokio::{
    signal,
    sync::{RwLock, RwLockWriteGuard},
};

mod hashring;
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

struct AppPutState {
    leveldb: Arc<record::LevelDb>,
    lock_keys: Arc<RwLock<HashSet<String>>>,
    client: reqwest::Client,
    hashring: Arc<HashRing<String>>,
    replicas: usize,
    subvolumes: u32,
    verify_checksums: bool,
}

struct AppGetState {
    leveldb: Arc<record::LevelDb>,
    client: reqwest::Client,
    hashring: Arc<HashRing<String>>,
    replicas: usize,
    subvolumes: u32,
}

struct AppDeleteState {
    leveldb: Arc<record::LevelDb>,
    lock_keys: Arc<RwLock<HashSet<String>>>,
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

    let leveldb = Arc::new(record::LevelDb::new(leveldb_path)?);
    let lock_keys = Arc::new(RwLock::new(HashSet::<String>::new()));

    let hashring = {
        let mut ring: HashRing<String> = HashRing::new();
        ring.batch_add(volumes);
        Arc::new(ring)
    };

    let client = reqwest::Client::new();

    let app_put_state = Arc::new(AppPutState {
        leveldb: leveldb.clone(),
        lock_keys: lock_keys.clone(),
        client: client.clone(),
        hashring: hashring.clone(),
        replicas,
        subvolumes,
        verify_checksums,
    });

    let app_get_state = Arc::new(AppGetState {
        leveldb: leveldb.clone(),
        client: client.clone(),
        hashring: hashring.clone(),
        replicas,
        subvolumes,
    });

    let app_delete_state = Arc::new(AppDeleteState {
        leveldb: leveldb.clone(),
        lock_keys: lock_keys.clone(),
    });

    let app = axum::Router::new()
        .route(
            "/:key",
            axum::routing::put(handle_put_record).with_state(app_put_state),
        )
        .route(
            "/:key",
            axum::routing::get(handle_get_record).with_state(app_get_state),
        )
        .route(
            "/:key",
            axum::routing::delete(handle_delete_record).with_state(app_delete_state),
        );

    let listener = tokio::net::TcpListener::bind(format!("[::]:{}", port)).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
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
    axum::extract::Path(key): axum::extract::Path<String>,
    axum::extract::State(state): axum::extract::State<Arc<AppPutState>>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> impl axum::response::IntoResponse {
    debug!("put_record: key: {}", key);

    if headers.get(axum::http::header::CONTENT_LENGTH).is_none() || body.is_empty() {
        return StatusCode::LENGTH_REQUIRED;
    }

    if state.lock_keys.read().await.contains(&key) {
        debug!("put_record: key: {} already locked", key);
        return StatusCode::CONFLICT;
    }

    let mut lock_keys = LevelDbKeyGuard::lock(&state.lock_keys, key.clone()).await;
    lock_keys.guard.insert(key.clone());

    let record = match state.leveldb.get_record_or_default(&key).await {
        Ok(record) => record,
        Err(e) => {
            error!(
                "put_record: failed to get record {} from leveldb: {}",
                key, e
            );
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    };

    if let record::Deleted::No = record.deleted() {
        return StatusCode::CONFLICT;
    }

    // TODO partNumber
    let replicas_volumes =
        hashring::get_volume(&key, &state.hashring, state.replicas, state.subvolumes);
    let mut futures = FuturesUnordered::new();
    for volume in replicas_volumes.clone() {
        let remote_replica_volume_path = record::get_remote_path(&key);
        let remote_url = format!("http://{}{}", volume, remote_replica_volume_path);
        debug!("put_record key: {} remote_url: {}", key, remote_url);
        let client_clone = state.client.clone();
        let value_clone = body.clone();
        futures.push(tokio::spawn(async move {
            remote_put(client_clone, remote_url, value_clone).await
        }));
    }

    while let Some(result) = futures.next().await {
        match result {
            Ok(_) => (),
            Err(e) => {
                error!(
                    "put_record: failed to put record {} in remote replica: {}",
                    key, e
                );

                // In case of error we want to mark the record as Deleted::Soft in the local leveldb
                let record =
                    record::Record::new(record::Deleted::Soft, String::new(), replicas_volumes);
                match state.leveldb.put_record(&key, record).await {
                    Ok(_) => (),
                    Err(e) => {
                        error!("put_record: failed to put record {} in leveldb: {}", key, e);
                        return StatusCode::INTERNAL_SERVER_ERROR;
                    }
                }
                return StatusCode::INTERNAL_SERVER_ERROR;
            }
        }
    }

    let value_md5_hash = if state.verify_checksums {
        let body_clone = body.clone();
        tokio::task::spawn_blocking(move || format!("{:x}", md5::compute(body_clone)))
            .await
            .unwrap_or_default()
    } else {
        String::new()
    };

    let record = record::Record::new(record::Deleted::No, value_md5_hash, replicas_volumes);
    match state.leveldb.put_record(&key, record).await {
        Ok(_) => (),
        Err(e) => {
            error!(
                "put_record: failed to put record with value_md5_hash {} in leveldb: {}",
                key, e
            );
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    }

    StatusCode::CREATED
}

async fn remote_put(
    client: reqwest::Client,
    remote_url: String,
    value: bytes::Bytes,
) -> anyhow::Result<()> {
    let res = client.put(remote_url.clone()).body(value).send().await?;
    if res.status().is_success() {
        if res.status().as_u16() != axum::http::StatusCode::CREATED.as_u16()
            && res.status().as_u16() != axum::http::StatusCode::NO_CONTENT.as_u16()
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
    axum::extract::Path(key): axum::extract::Path<String>,
    axum::extract::State(state): axum::extract::State<Arc<AppGetState>>,
) -> axum::response::Response {
    debug!("get_record: key: {}", key);

    let record = {
        match state.leveldb.get_record(&key).await {
            Ok(record) => record,
            Err(e) => {
                error!(
                    "get_record: failed to get record {} from leveldb: {}",
                    key, e
                );
                return axum::http::Response::builder()
                    .status(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
                    .body(axum::body::Body::empty())
                    .unwrap();
            }
        }
    };

    if record.is_none() {
        return axum::http::Response::builder()
            .status(axum::http::StatusCode::NOT_FOUND)
            .header(axum::http::header::CONTENT_LENGTH, "0")
            .header("Content-Md5", "")
            .body(axum::body::Body::empty())
            .unwrap();
    }

    let record = record.unwrap();

    // TODO fallbacks
    if record.deleted() != record::Deleted::No {
        debug!(
            "get_record: key: {} not found, record deleted: {:?}",
            key,
            record.deleted()
        );
        return axum::http::Response::builder()
            .status(axum::http::StatusCode::NOT_FOUND)
            .header(axum::http::header::CONTENT_LENGTH, "0")
            .header("Content-Md5", record.hash().to_string())
            .body(axum::body::Body::empty())
            .unwrap();
    }

    let replicas_volumes =
        hashring::get_volume(&key, &state.hashring, state.replicas, state.subvolumes);
    let needs_rebalance_header = if needs_rebalance(&replicas_volumes, record.read_volumes()) {
        "unbalanced"
    } else {
        "balanced"
    };

    let remote_url: Option<String> = {
        let mut found_remote_url = None;
        let mut rnd = rand::rngs::StdRng::from_entropy();
        for volume in replicas_volumes.choose(&mut rnd).into_iter() {
            let remote_replica_volume_path = record::get_remote_path(&key);
            let remote_url = format!("http://{}{}", volume, remote_replica_volume_path);
            if let Ok(()) = remote_head(&state.client, &remote_url).await {
                found_remote_url = Some(remote_url);
                break;
            }
        }
        found_remote_url
    };

    match remote_url {
        Some(remote_url) => {
            debug!("get_record: key: {} from remote_url: {}", key, remote_url);
            axum::http::Response::builder()
                .status(axum::http::StatusCode::FOUND)
                .header(axum::http::header::LOCATION, remote_url)
                .header(axum::http::header::CONTENT_LENGTH, "0")
                .header("Content-Md5", record.hash().to_string())
                .body(axum::body::Body::empty())
                .unwrap()
        }
        None => {
            debug!("get_record: key: {} not found in any volume", key);
            axum::http::Response::builder()
                .status(axum::http::StatusCode::GONE)
                .header(axum::http::header::CONTENT_LENGTH, "0")
                .header("Key-Volumes", record.read_volumes().join(","))
                .header("Key-Balance", needs_rebalance_header)
                .body(axum::body::Body::empty())
                .unwrap()
        }
    }
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
    axum::extract::Path(key): axum::extract::Path<String>,
    axum::extract::State(state): axum::extract::State<Arc<AppDeleteState>>,
) -> axum::response::Response {
    debug!("delete_record: key: {}", key);

    if state.lock_keys.read().await.contains(&key) {
        debug!("delete_record: key: {} already locked", key);
        return axum::http::Response::builder()
            .status(axum::http::StatusCode::CONFLICT)
            .body(axum::body::Body::empty())
            .unwrap();
    }

    let mut lock_keys = LevelDbKeyGuard::lock(&state.lock_keys, key.clone()).await;
    lock_keys.guard.insert(key.clone());

    let record = match state.leveldb.get_record_or_default(&key).await {
        Ok(record) => record,
        Err(e) => {
            error!(
                "delete_record: failed to get record {} from leveldb: {}",
                key, e
            );
            return axum::http::Response::builder()
                .status(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
                .body(axum::body::Body::empty())
                .unwrap();
        }
    };

    // TODO unlink and soft delete, for now we assume soft is always deleted
    // This probalby will make some tests fail with link/unlink
    if record.deleted() == record::Deleted::Hard || record.deleted() == record::Deleted::Soft {
        debug!("delete_record: key: {} already deleted", key);
        return axum::http::Response::builder()
            .status(axum::http::StatusCode::NOT_FOUND)
            .body(axum::body::Body::empty())
            .unwrap();
    }

    let deleted_record = record::Record::new(
        record::Deleted::Soft,
        record.hash().to_string(),
        record.read_volumes().to_vec(),
    );
    match state.leveldb.put_record(&key, deleted_record).await {
        Ok(_) => (),
        Err(e) => {
            error!(
                "delete_record: failed to put deleted record {} in leveldb: {}",
                key, e
            );
            return axum::http::Response::builder()
                .status(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
                .body(axum::body::Body::empty())
                .unwrap();
        }
    }

    // TODO unlink

    axum::http::Response::builder()
        .status(axum::http::StatusCode::NO_CONTENT)
        .body(axum::body::Body::empty())
        .unwrap()
}
