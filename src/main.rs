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

#[derive(Debug)]
struct App {
    port: u16,
    ldb_path: String,
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

    let app = App {
        port: cli.port,
        ldb_path: cli.ldb,
        md5sum: cli.md5sum,
    };

    dbg!(&app);

    let api = warp_handlers();
    warp::serve(api).run(([127, 0, 0, 1], app.port)).await;

    Ok(())
}

fn warp_handlers() -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    let put_record = warp::path::param()
        .and(warp::put())
        .and(warp::body::bytes())
        .and(warp::path::end())
        .map(handle_put_record);

    let get_record = warp::path::param()
        .and(warp::get())
        .and(warp::path::end())
        .map(handle_get_record);

    put_record.or(get_record)
}

fn handle_put_record(key: String, value: bytes::Bytes) -> impl warp::Reply {
    debug!("put_record: key: {}, value: {:?}", key, value);
    warp::http::Response::builder()
        .status(warp::http::StatusCode::NOT_IMPLEMENTED)
        .body(value)
}

fn handle_get_record(key: String) -> impl warp::Reply {
    debug!("get_record: key: {}", key);
    warp::http::Response::builder()
        .status(warp::http::StatusCode::NOT_IMPLEMENTED)
        .body("todo")
}
