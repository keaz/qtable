use std::{collections::HashMap, sync::Arc};

use log::debug;
use tokio::{
    io,
    net::TcpListener,
    sync::{mpsc::UnboundedSender, RwLock},
};

use crate::database::NoSqlDatabase;

use super::client;

pub struct Server {
    pub port: u16,
}

impl Server {
    pub fn new(port: u16) -> Server {
        Server { port }
    }
}

impl Server {
    pub async fn run(
        &self,
        data_path: String,
        database: Arc<RwLock<HashMap<String, NoSqlDatabase>>>,
    ) {
        let listener = TcpListener::bind(format!("0.0.0.0:{}", self.port))
            .await
            .unwrap();
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let database = database.clone();
            let data_path = data_path.clone();
            tokio::spawn(async move {
                debug!("New connection from: {}", socket.peer_addr().unwrap());
                let (reader, writer) = io::split(socket);
                let mut client = client::Client::new(data_path, reader, writer, database);

                client.listen().await;
            });
        }
    }
}
