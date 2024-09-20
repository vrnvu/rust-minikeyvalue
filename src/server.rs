use std::{collections::HashSet, path::Path, sync::Arc};

use axum::http::StatusCode;
use futures::{stream::FuturesUnordered, StreamExt};
use log::{debug, error};
use parking_lot::RwLock;
use rand::{seq::SliceRandom, SeedableRng};
use tokio::signal;

use crate::{hashring, record};

struct AppPutState {
    leveldb: Arc<record::LevelDb>,
    lock_keys: Arc<RwLock<HashSet<String>>>,
    client: reqwest::Client,
    hashring: Arc<hashring::Ring>,
    verify_checksums: bool,
}

struct AppGetState {
    leveldb: Arc<record::LevelDb>,
    client: reqwest::Client,
    hashring: Arc<hashring::Ring>,
}

struct AppDeleteState {
    leveldb: Arc<record::LevelDb>,
    lock_keys: Arc<RwLock<HashSet<String>>>,
}

pub async fn new_and_serve(
    port: u16,
    leveldb_path: &Path,
    verify_checksums: bool,
    volumes: Vec<String>,
    replicas: usize,
    subvolumes: u32,
) -> anyhow::Result<()> {
    let leveldb = Arc::new(record::LevelDb::new(leveldb_path)?);
    let lock_keys = Arc::new(RwLock::new(HashSet::<String>::new()));

    let hashring = {
        let hashring = hashring::Ring::new(volumes, replicas, subvolumes);
        Arc::new(hashring)
    };

    let client = reqwest::Client::new();

    let app_put_state = Arc::new(AppPutState {
        leveldb: leveldb.clone(),
        lock_keys: lock_keys.clone(),
        client: client.clone(),
        hashring: hashring.clone(),
        verify_checksums,
    });

    let app_get_state = Arc::new(AppGetState {
        leveldb: leveldb.clone(),
        client: client.clone(),
        hashring: hashring.clone(),
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

    if state.lock_keys.read().contains(&key) {
        debug!("put_record: key: {} already locked", key);
        return StatusCode::CONFLICT;
    }

    state.lock_keys.write().insert(key.clone());

    let record = match state.leveldb.get_record_or_default(&key).await {
        Ok(record) => record,
        Err(e) => {
            error!(
                "put_record: failed to get record {} from leveldb: {}",
                key, e
            );
            state.lock_keys.write().remove(&key);
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    };

    if let record::Deleted::No = record.deleted() {
        state.lock_keys.write().remove(&key);
        return StatusCode::CONFLICT;
    }

    // TODO partNumber
    let replicas_volumes = state.hashring.get_volume(&key);

    let mut futures = FuturesUnordered::new();
    for volume in replicas_volumes.iter() {
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
                        state.lock_keys.write().remove(&key);
                        return StatusCode::INTERNAL_SERVER_ERROR;
                    }
                }
                state.lock_keys.write().remove(&key);
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
            state.lock_keys.write().remove(&key);
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    }

    state.lock_keys.write().remove(&key);
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

    let replicas_volumes = state.hashring.get_volume(&key);
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

    if state.lock_keys.read().contains(&key) {
        debug!("delete_record: key: {} already locked", key);
        return axum::http::Response::builder()
            .status(axum::http::StatusCode::CONFLICT)
            .body(axum::body::Body::empty())
            .unwrap();
    }

    state.lock_keys.write().insert(key.clone());

    let record = match state.leveldb.get_record_or_default(&key).await {
        Ok(record) => record,
        Err(e) => {
            error!(
                "delete_record: failed to get record {} from leveldb: {}",
                key, e
            );
            state.lock_keys.write().remove(&key);
            return axum::http::Response::builder()
                .status(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
                .body(axum::body::Body::empty())
                .unwrap();
        }
    };

    if record.deleted() == record::Deleted::Hard || record.deleted() == record::Deleted::Soft {
        debug!("delete_record: key: {} already deleted", key);
        state.lock_keys.write().remove(&key);
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
            state.lock_keys.write().remove(&key);
            return axum::http::Response::builder()
                .status(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
                .body(axum::body::Body::empty())
                .unwrap();
        }
    }

    state.lock_keys.write().remove(&key);
    axum::http::Response::builder()
        .status(axum::http::StatusCode::NO_CONTENT)
        .body(axum::body::Body::empty())
        .unwrap()
}
