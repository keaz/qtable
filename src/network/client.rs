use std::{collections::HashMap, sync::Arc};

use bincode::de;
use log::{debug, error, info};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf},
    net::TcpStream,
    sync::RwLock,
};

use crate::{
    database::NoSqlDatabase,
    parser::{handle_message, parse_create_command, Command, Query, CREATE},
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
                                self.writer.write_all(b"Database already exists").await.unwrap();
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
                    Command::Select(query) => todo!(),
                    Command::Insert(insert_data) => todo!(),
                    Command::Update(insert_data, query) => todo!(),
                    Command::Delete(query) => todo!(),
                    Command::Create(_) => {
                        error!("Unexpected create command");
                        buffer.clear();
                        //#TODO: send  error response
                    }
                    Command::Define(table, definitions) => todo!(),
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
}
