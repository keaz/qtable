use std::sync::Arc;

use database::NoSqlDatabase;
use tokio::sync::RwLock;

mod config;
mod data_object;
mod database;
mod index;
mod network;
mod parser;

lazy_static::lazy_static! {
    static ref CONFIG: config::ServerConfig = config::ServerConfig::new().unwrap();
}

fn main() {
    log4rs::init_file("config/log4rs.yml", Default::default()).unwrap();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
        .block_on(start());
}

async fn start() {
    let data_path = CONFIG.data_path.clone();
    let port = CONFIG.port.unwrap_or(8080);
    let server = network::server::Server::new(port);
    let databases = NoSqlDatabase::load_databases(&data_path).await.unwrap();
    let databases = Arc::new(RwLock::new(databases));

    server.run(data_path, databases.clone()).await;
}
