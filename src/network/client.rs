use std::{collections::HashMap, sync::Arc};

use bincode::serialize;
use log::{debug, error, info};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf},
    net::TcpStream,
    sync::RwLock,
};

use crate::{
    database::NoSqlDatabase,
    parser::{
        handle_message, parse_create_command, Command, Definition, InsertData, Query, CREATE,
    },
};

pub struct Client {
    data_path: String,
    reader: ReadHalf<TcpStream>,
    writer: WriteHalf<TcpStream>,
    databases: Arc<RwLock<HashMap<String, NoSqlDatabase>>>,
}

impl Client {
    pub fn new(
        data_path: String,
        reader: ReadHalf<TcpStream>,
        writer: WriteHalf<TcpStream>,
        databases: Arc<RwLock<HashMap<String, NoSqlDatabase>>>,
    ) -> Client {
        Client {
            data_path,
            reader,
            writer,
            databases,
        }
    }
}

impl Client {
    pub async fn listen(&mut self) {
        let mut buffer = Vec::with_capacity(1024);

        while let Ok(n) = self.reader.read_buf(&mut buffer).await {
            if n == 0 {
                break;
            }

            let message = String::from_utf8_lossy(&buffer);
            debug!("Received message: {}", message);

            if message.starts_with(CREATE) {
                let create_command = parse_create_command(&message);
                debug!("Create command: {:?}", create_command);
                match create_command {
                    Ok(command) => {
                        if let Command::Create(database_to_create) = command {
                            let databases = self.databases.read().await;
                            let database = databases.get(&database_to_create);
                            if database.is_some() {
                                error!("Database {} is already exists", database_to_create);
                                buffer.clear();
                                //#TODO: Handle the error and send the response
                                self.writer
                                    .write_all(b"Database already exists")
                                    .await
                                    .unwrap();
                                continue;
                            }
                            drop(databases);
                            let new_database =
                                NoSqlDatabase::new(&database_to_create, &self.data_path).await;
                            match new_database {
                                Ok(database) => {
                                    let mut databases = self.databases.write().await;
                                    debug!("Database {} created", database_to_create);
                                    databases.insert(database_to_create, database);
                                    buffer.clear();
                                    self.writer.write_all(b"Database created").await.unwrap();
                                    self.writer.flush().await.unwrap();
                                }
                                Err(error) => {
                                    error!("Failed to create databse {}", error);
                                    buffer.clear();
                                    continue;
                                    //#TODO: Handle the error and send the response
                                }
                            }
                        }
                    }
                    Err(error) => {
                        error!("Error parsing create command: {}", error);
                    }
                }
                continue;
            }
            let index = message.find(':');
            if index.is_none() {
                info!("Invalid message format"); // should handle correctly
                buffer.clear();
                continue;
            }
            let db = message[..index.unwrap()].trim();
            let message = message[index.unwrap() + 1..].trim();
            debug!("Parsed message: database {}, command {}", db, message);
            let command = handle_message(db, message);
            debug!("Command: {:?}", command);
            match command {
                Ok(command) => match command {
                    Command::Select(query) => {
                        self.handle_select(query).await;
                    }
                    Command::Insert(insert_data) => {
                        self.handle_insert(db,insert_data).await;
                    }
                    Command::Update(insert_data, query) => {
                        self.handle_update(db,insert_data, query).await;
                    }
                    Command::Delete(query) => {
                        self.handle_delete(db,query).await;
                    }
                    Command::Create(_) => {
                        error!("Unexpected create command");
                        buffer.clear();
                        //#TODO: send  error response
                    }
                    Command::Define(db, table, definitions) => {
                        self.handle_definition(db, table, definitions).await;
                    }
                    Command::Alter => todo!(),
                    Command::Drop => todo!(),
                },
                Err(error) => {
                    error!("Error parsing message {}", error);
                    buffer.clear();
                    //#TODO: Send error response
                }
            }
            buffer.clear();
        }
        info!("Connection closed")
    }

    async fn handle_definition(
        &mut self,
        db: String,
        table: String,
        definitions: HashMap<String, Definition>,
    ) {
        let mut databases = self.databases.write().await;
        let database = databases.get_mut(&db);
        match database {
            Some(database) => {
                let response = database.handle_definition(table, definitions).await;
                let response = serialize(&response).unwrap();
                self.writer.write_all(&response).await.unwrap();
            }
            None => {
                self.writer.write_all(b"No Records found").await.unwrap();
            }
        }
    }

    async fn handle_delete(&mut self,db: &str, delete_query: Query) {
        let mut databases = self.databases.write().await;
        let database = databases.get_mut(db);
        match database {
            Some(database) => {
                let response = database.handle_delete(delete_query).await;
                let response = serialize(&response).unwrap();
                self.writer.write_all(&response).await.unwrap();
            }
            None => {
                self.writer.write_all(b"No Records found").await.unwrap();
            }
        }
    }

    async fn handle_update(&mut self,db: &str, insert_data: InsertData, query: Query) {
        let mut databases = self.databases.write().await;
        let database = databases.get_mut(db);
        match database {
            Some(database) => {
                let response = database.handle_update(insert_data, query).await;
                let response = serialize(&response).unwrap();
                self.writer.write_all(&response).await.unwrap();
            }
            None => {
                self.writer.write_all(b"No Records found").await.unwrap();
            }
        }
    }

    async fn handle_insert(&mut self,db: &str, insert_data: InsertData) {
        let mut databases = self.databases.write().await;
        let database = databases.get_mut(db);
        match database {
            Some(database) => {
                let response = database.handle_insert(insert_data).await;
                let response = serialize(&response).unwrap();
                self.writer.write_all(&response).await.unwrap();
            }
            None => {
                self.writer.write_all(b"No Records found").await.unwrap();
            }
        }
    }

    async fn handle_select(&mut self, query: Query) {
        let databases = self.databases.read().await;
        let database = databases.get(&query.db);
        match database {
            Some(database) => {
                let response = database.handle_query(query).await;
                let response = serialize(&response).unwrap();
                self.writer.write_all(&response).await.unwrap();
            }
            None => {
                self.writer.write_all(b"No Records found").await.unwrap();
            }
        }
    }
}
