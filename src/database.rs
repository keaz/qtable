use std::{collections::HashMap, path::Path};

use serde::Serialize;
use tokio::fs;
use walkdir::WalkDir;

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

#[derive(Debug, Serialize)]
pub enum DataResponse {
    Data(Vec<InsertData>),
    Error(String),
}

impl ToString for DataResponse {
    fn to_string(&self) -> String {
        match self {
            DataResponse::Data(data) => {
                let mut data_string = String::new();
                for d in data {
                    data_string.push_str(&format!("{:?}", d));
                    data_string.push_str("\n");
                }
                data_string
            }
            DataResponse::Error(err) => err.to_string(),
        }
    }
}

impl NoSqlDatabase {
    pub async fn new(data_base: &str, data_path: &str) -> Result<Self, String> {
        let root_path = format!("{}/{}", data_path, data_base);
        let path = Path::new(root_path.as_str());
        if path.exists() {
            return Err(format!("Database {} already exists", data_base));
        }
        fs::create_dir_all(root_path.as_str()).await.unwrap();

        Ok(NoSqlDatabase {
            data_objects: HashMap::new(),
            data_base: data_base.to_string(),
            root_path: data_path.to_string(),
        })
    }

    async fn load(root_dir: &str, database: &str) -> Result<Self, String> {
        let path = Path::new(root_dir).join(database);
        if !path.exists() {
            return Err(format!("Database {} does not exist", path.to_str().unwrap()));
        }

        let mut data_objects = HashMap::new();
        for entry in WalkDir::new(path.clone()).max_depth(1) {
            let entry = entry.unwrap();
            // skip the root path
            if entry.path() == path {
                continue;
            }
            if entry.file_type().is_dir() {
                let table = entry.file_name().to_str().unwrap().to_string();
                let data_object = NoSqlDataObject::load(&table, path.to_str().unwrap()).await.unwrap();
                data_objects.insert(table, data_object);
            }
        }

        Ok(NoSqlDatabase {
            data_objects,
            data_base: database.to_string(),
            root_path: root_dir.to_string(),
        })
    }

    pub async fn load_databases(root_dir: &str) -> Result<HashMap<String, Self>, String> {
        let mut databases = HashMap::new();
        let path = Path::new(root_dir);
        if !path.exists() {
            return Ok(databases);
        }
        for entry in WalkDir::new(path).max_depth(1) {
            let entry = entry.unwrap();
            // skip the root path
            if entry.path() == path {
                continue;
            }
            if entry.file_type().is_dir() {
                let database = entry.file_name().to_str().unwrap().to_string();
                let database = NoSqlDatabase::load(root_dir, &database).await.unwrap();
                databases.insert(database.data_base.clone(), database);
            }
        }

        Ok(databases)
    }

    pub async fn handle_message(&mut self, message: &str) -> DataResponse {
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
                crate::parser::Command::Create(_) => DataResponse::Error(
                    "Something went wrong, create should not come here ".to_string(),
                ),
                crate::parser::Command::Define(_,table, definition) => {
                    self.handle_definition(table, definition).await
                }
                crate::parser::Command::Alter => todo!(),
                crate::parser::Command::Drop => todo!(),
            },
            Err(e) => DataResponse::Error(format!("Error parsing message: {}", e)),
        }
    }

    pub async fn handle_definition(
        &mut self,
        table: String,
        definition: HashMap<String, Definition>,
    ) -> DataResponse {
        let data_object = NoSqlDataObject::new(&table, format!("{}/{}",self.root_path,self.data_base).as_str(), definition).await;
        match data_object {
            Ok(data_object) => {
                self.data_objects.insert(table, data_object);
                DataResponse::Data(vec![])
            }
            Err(err) => DataResponse::Error(format!("Error creating table: {}", err)),
        }
    }

    pub async fn handle_delete(&mut self, delete_query: Query) -> DataResponse {
        let table = delete_query.table_name.as_str();
        if let Some(data_object) = self.data_objects.get_mut(table) {
            let result = data_object.handle_delete(&delete_query).await;
            match result {
                Ok(_) => DataResponse::Data(vec![]),
                Err(e) => DataResponse::Error(format!("Error deleting data: {}", e)),
            }
        } else {
            DataResponse::Error(format!("Table {} not found", table))
        }
    }

    pub async fn handle_update(&mut self, update_data: InsertData, query: Query) -> DataResponse {
        let table = update_data.table.as_str();
        if let Some(data_object) = self.data_objects.get_mut(&update_data.table) {
            let result = data_object.handle_update(&update_data, query).await;
            match result {
                Ok(_) => DataResponse::Data(vec![update_data]),
                Err(e) => DataResponse::Error(format!("Error updating data: {}", e)),
            }
        } else {
            DataResponse::Error(format!("Table {} not found", table))
        }
    }

    pub async fn handle_insert(&mut self, insert_data: InsertData) -> DataResponse {
        let table = insert_data.table.as_str();
        if let Some(data_object) = self.data_objects.get_mut(&insert_data.table) {
            let result = data_object.handle_insert(&insert_data).await;
            match result {
                Ok(_) => DataResponse::Data(vec![insert_data]),
                Err(e) => DataResponse::Error(format!("Error inserting data: {}", e)),
            }
        } else {
            DataResponse::Error(format!("Table {} not found", table))
        }
    }

    pub async fn handle_query(&self, query: Query) -> DataResponse {
        if let Some(data_object) = self.data_objects.get(&query.table_name) {
            let query_data = data_object.handle_query(&query.filter).await;
            match query_data {
                Ok(data) => DataResponse::Data(data),
                Err(e) => DataResponse::Error(format!("Error Quering data {}", e)),
            };
        }
        DataResponse::Error(format!("Table {} not found", query.table_name))
    }
}

#[cfg(test)]
mod test {

    use tempfile::Builder;

    use super::*;
    use crate::data_object::NoSqlDataObject;
    use std::{collections::HashMap, fs::File};

    #[tokio::test]
    async fn test_new_database() {
        let dir = Builder::new()
            .prefix("data")
            .tempdir()
            .expect("Failed to create temp directory");

        let _file = File::create(dir.path());
        let root_dir = dir.path().to_str().unwrap();

        let _ = NoSqlDatabase::new("test", root_dir).await.unwrap();
        let database_path = dir.path().join("test");
        assert!(database_path.exists());
    }

    #[tokio::test]
    async fn test_load_database() {
        let dir = Builder::new()
            .prefix("data")
            .tempdir()
            .expect("Failed to create temp directory");

        let _file = File::create(dir.path());
        let root_dir = dir.path().to_str().unwrap();

        let database = NoSqlDatabase::new("test", root_dir).await.unwrap();
        let loaded_database = NoSqlDatabase::load(root_dir, "test").await.unwrap();
        assert_eq!(database.data_base, loaded_database.data_base);
        assert_eq!(database.root_path, loaded_database.root_path);
    }
}
