mod data_object;
mod database;
mod index;
mod network;
mod parser;


fn main() {
    log4rs::init_file("config/log4rs.yml", Default::default()).unwrap();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
        .block_on(start());
}

async fn start(){
    let server = network::server::Server::new(8080);
    server.run().await;
}