use std::{collections::HashMap, path::Path};

use nom::Err;
use serde::Serialize;
use tokio::fs;

use crate::{
    data_object::{self, NoSqlDataObject},
    parser::{handle_message, Definition, InsertData, Query},
};

pub struct NoSqlDatabase {
    data_objects: HashMap<String, NoSqlDataObject>,
    data_base: String,
    root_path: String,
}

#[derive(Debug, Serialize)]
struct Response {
    data: Option<Vec<InsertData>>,
    error: Option<String>,
}

impl NoSqlDatabase {
    pub async fn new(data_dir: String, data_path: String) -> Result<Self,String> {

        let root_path = format!("{}/{}", data_path, data_dir);
        let path = Path::new(root_path.as_str());
        if path.exists() {
            return Err(format!("Database {} already exists", data_dir));
        }
        fs::create_dir_all(root_path.as_str()).await.unwrap();

        Ok(NoSqlDatabase {
            data_objects: HashMap::new(),
            data_base: data_dir,
            root_path,
        })
    }

    pub async fn load(root_dir: &str) -> Result<Self, String> {
        let path = Path::new(root_dir);
        if !path.exists() {
            return Err(format!("Database {} does not exist", root_dir));
        }

        let mut data_objects = HashMap::new();
        
    }

    pub async fn handle_message(&mut self, message: &str) -> Response {
        let message = handle_message(&self.data_base, message);
        match message {
            Ok(message) => match message {
                crate::parser::Command::Select(query) => self.handle_query(query).await,
                crate::parser::Command::Insert(insert_data) => {
                    self.handle_insert(insert_data).await
                }
                crate::parser::Command::Update(insert_data, query) => {
                    self.handle_update(insert_data, query).await
                }
                crate::parser::Command::Delete(delete_query) => {
                    self.handle_delete(delete_query).await
                }
                crate::parser::Command::Create(_) => Response {
                    data: None,
                    error: Some("Something went wrong, create should not come here ".to_string()),
                },
                crate::parser::Command::Define(table, definition) => {
                    self.handle_definition(table, definition).await
                }
                crate::parser::Command::Alter => todo!(),
                crate::parser::Command::Drop => todo!(),
            },
            Err(e) => Response {
                data: None,
                error: Some(format!("Error parsing message: {}", e)),
            },
        }
    }

    async fn handle_definition(
        &mut self,
        table: String,
        definition: HashMap<String, Definition>,
    ) -> Response {
        let data_object = NoSqlDataObject::new(&table, self.root_path.as_str(), definition).await;
        match data_object {
            Ok(data_object) => {
                self.data_objects.insert(table, data_object);
                Response {
                    data: None,
                    error: None,
                }
            }
            Err(err) => Response {
                data: None,
                error: Some(format!("Error creating table: {}", err)),
            },
        }
    }

    async fn handle_delete(&mut self, delete_query: Query) -> Response {
        let table = delete_query.table_name.as_str();
        if let Some(data_object) = self.data_objects.get_mut(table) {
            let result = data_object.handle_delete(&delete_query).await;
            match result {
                Ok(_) => Response {
                    data: None,
                    error: None,
                },
                Err(e) => Response {
                    data: None,
                    error: Some(format!("Error deleting data: {}", e)),
                },
            }
        } else {
            Response {
                data: None,
                error: Some(format!("Table {} not found", table)),
            }
        }
    }

    async fn handle_update(&mut self, update_data: InsertData, query: Query) -> Response {
        let table = update_data.table.as_str();
        if let Some(data_object) = self.data_objects.get_mut(&update_data.table) {
            let result = data_object.handle_update(&update_data, query).await;
            match result {
                Ok(_) => Response {
                    data: Some(vec![update_data]),
                    error: None,
                },
                Err(e) => Response {
                    data: None,
                    error: Some(format!("Error updating data: {}", e)),
                },
            }
        } else {
            Response {
                data: None,
                error: Some(format!("Table {} not found", table)),
            }
        }
    }

    async fn handle_insert(&mut self, insert_data: InsertData) -> Response {
        let table = insert_data.table.as_str();
        if let Some(data_object) = self.data_objects.get_mut(&insert_data.table) {
            let result = data_object.handle_insert(&insert_data).await;
            match result {
                Ok(_) => Response {
                    data: Some(vec![insert_data]),
                    error: None,
                },
                Err(e) => Response {
                    data: None,
                    error: Some(format!("Error inserting data: {}", e)),
                },
            }
        } else {
            Response {
                data: None,
                error: Some(format!("Table {} not found", table)),
            }
        }
    }

    async fn handle_query(&self, query: Query) -> Response {
        if let Some(data_object) = self.data_objects.get(&query.table_name) {
            let query_data = data_object.handle_query(&query.filter).await;
            match query_data {
                Ok(data) => Response {
                    data: Some(data),
                    error: None,
                },
                Err(e) => Response {
                    data: None,
                    error: Some(format!("Error querying data: {}", e)),
                },
            };
        }
        Response {
            data: None,
            error: Some(format!("Table {} not found", query.table_name)),
        }
    }
}
