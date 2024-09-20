use std::path::Path;

use clap::Parser;

mod hashring;
mod record;
mod server;

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

    server::new_and_serve(
        port,
        leveldb_path,
        verify_checksums,
        volumes,
        replicas,
        subvolumes,
    )
    .await?;

    Ok(())
}
