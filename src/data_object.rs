use std::{collections::HashMap, fmt::{Display, Formatter}, io::SeekFrom};

use log::{debug, error};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

use crate::{
    index::{Index, IndexId},
    parser::{Condition, InsertData, Query, WildCardOperations},
};

const OBJECT_ID: &str = "object_id";

pub struct NoSqlDataObject {
    data_object: String,
    index: HashMap<String, Index>, // Attribute, Index
    root: String,
}

pub enum RangeOp {
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
}
#[derive(Debug)]
pub enum SerializeError {
    Serialize(String),
    Deserialize(String),
    Update(String),
    Insert(String),
    Delete(String),
}

impl Display for SerializeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SerializeError::Serialize(e) => write!(f, "Serialize Error: {}", e),
            SerializeError::Deserialize(e) => write!(f, "Deserialize Error: {}", e),
            SerializeError::Update(e) => write!(f, "Update Error: {}", e),
            SerializeError::Insert(e) => write!(f, "Insert Error: {}", e),
            SerializeError::Delete(e) => write!(f, "Delete Error: {}", e),
        }
    }
}



impl NoSqlDataObject {
    pub fn add_to_index(&mut self, attribute: &str, value: &str, object_id: &IndexId) {
        if let Some(index) = self.index.get_mut(attribute) {
            index.add_to_index(value, object_id);
        }
    }

    pub fn remove_from_index(&mut self, attribute: &str, value: &str, object_id: &IndexId) {
        if let Some(index) = self.index.get_mut(attribute) {
            index.remove_from_index(value, object_id);
        }
    }

    pub async fn handle_query(&self, condition: &Condition) -> Result<Vec<InsertData>, SerializeError> {
        let object_ids = self.query(condition);
        self.get_record(object_ids).await
    }

    fn query(&self, condition: &Condition) -> Vec<&IndexId> {
        match condition {
            Condition::WildCard(op) => self.query_wildcard(op),
            Condition::Equal(attr, value) => self.query_equal(attr, value),
            Condition::GreaterThan(attr, value) => {
                self.query_range(attr, value, RangeOp::GreaterThan)
            }
            Condition::GreaterThanOrEqual(attr, value) => {
                self.query_range(attr, value, RangeOp::GreaterThanOrEqual)
            }
            Condition::LessThan(attr, value) => self.query_range(attr, value, RangeOp::LessThan),
            Condition::LessThanOrEqual(attr, value) => {
                self.query_range(attr, value, RangeOp::LessThanOrEqual)
            }
            Condition::And(cond1, cond2) => {
                let mut results1 = self.query(cond1);
                let results2 = self.query(cond2);
                results1.retain(|item| results2.contains(item));
                results1
            }
            Condition::Or(cond1, cond2) => {
                let mut results1 = self.query(cond1);
                let results2 = self.query(cond2);
                results1.extend(results2);
                results1.dedup();
                results1
            }
        }
    }

    fn query_wildcard(&self, op: &WildCardOperations) -> Vec<&IndexId> {
        match op {
            WildCardOperations::StartsWith(attr, prefix) => self.query_prefix(attr, prefix),
            WildCardOperations::EndsWith(attr, suffix) => self.query_suffix(attr, suffix),
            WildCardOperations::Contains(attr, substring) => self.query_contains(attr, substring),
        }
    }

    fn query_equal(&self, attr: &str, value: &str) -> Vec<&IndexId> {
        if let Some(index) = self.index.get(attr) {
            return index.query_equal(value);
        }
        vec![]
    }

    fn query_range(&self, attr: &str, value: &str, op: RangeOp) -> Vec<&IndexId> {
        if let Some(index) = self.index.get(attr) {
            return index.query_range(value, op);
        }
        vec![]
    }

    fn query_prefix(&self, attr: &str, prefix: &str) -> Vec<&IndexId> {
        if let Some(index) = self.index.get(attr) {
            return index.query_prefix(prefix);
        }
        vec![]
    }

    fn query_suffix(&self, attr: &str, suffix: &str) -> Vec<&IndexId> {
        if let Some(index) = self.index.get(attr) {
            return index.query_suffix(suffix);
        }
        vec![]
    }

    fn query_contains(&self, attr: &str, substring: &str) -> Vec<&IndexId> {
        if let Some(index) = self.index.get(attr) {
            return index.query_contains(substring);
        }
        vec![]
    }

    pub async fn handle_insert(&mut self, insert_data: &InsertData) -> Result<(), SerializeError> {
        let index_id = self.insert_record(insert_data).await?;
        self.add_to_index(OBJECT_ID, &insert_data.data_object_id, &index_id); //# FIXME: should add other attributes to the index considering the definition
        Ok(())
    }

    pub async fn handle_update(&mut self, update_data: &InsertData, query: Query) -> Result<(), SerializeError> {
        let index_id = self.query(&query.filter);
        if index_id.is_empty() {
            return Err(SerializeError::Update("Data not found".to_string()));
        }
        let index_id = index_id[0];
        let new_index_id = self.update_record(index_id.position, index_id.length, &update_data).await?;
        self.add_to_index(OBJECT_ID, &update_data.data_object_id, &new_index_id);
        Ok(())
    }
}

impl NoSqlDataObject {
    async fn insert_record(&mut self, insert_data: &InsertData) -> Result<IndexId, SerializeError> {
        let serialized = bincode::serialize(&insert_data);
        match serialized {
            Ok(data) => {
                let data_file_name = format!("{}/{}.dat", self.root, self.data_object);
                let file = File::options().append(true).open(data_file_name).await; // Data file
                                                                                    // should be available at this point
                match file {
                    Ok(file) => {
                        let data_len = data.len();
                        let (position, _file) = self.write_to_end(file, data).await?;
                        Ok(IndexId {
                            position,
                            length: data_len,
                        })
                    }
                    Err(e) => {
                        error!("Error: {:?}", e);
                        Err(SerializeError::Insert(
                            "Error opening data file".to_string(),
                        ))
                    }
                }
            }
            Err(e) => {
                error!("Error: {:?}", e);
                Err(SerializeError::Serialize("Error serializing data".to_string()))
            }
        }
    }

    async fn get_record(
        &self,
        data_objects: Vec<&IndexId>,
    ) -> Result<Vec<InsertData>, SerializeError> {
        let data_file_name = format!("{}/{}.dat", self.root, self.data_object);
        let file = File::open(data_file_name).await;
        let mut data = vec![];
        match file {
            Ok(mut file) => {
                debug!("Data file opened");

                for data_object in data_objects {
                    file.seek(SeekFrom::Start(data_object.position))
                        .await
                        .unwrap();
                    let mut data_chunk = vec![0; data_object.length];
                    file.read_exact(&mut data_chunk).await.unwrap();
                    let data_object = bincode::deserialize::<InsertData>(&data_chunk);
                    match data_object {
                        Ok(data_object) => data.push(data_object),
                        Err(e) => {
                            error!("Error: {:?}", e);
                            return Err(SerializeError::Deserialize("Error deserializing data".to_string()));
                        }
                    }
                }
            }
            Err(e) => {
                error!("Error: {:?}", e);
                return Err(SerializeError::Serialize("Error opening data file".to_string()));
            }
        }
        Ok(data)
    }

    async fn get_data_object(&self, data_object: &IndexId) -> Result<InsertData, SerializeError> {
        let data_file_name = format!("{}/{}.dat", self.root, self.data_object);
        let file = File::open(data_file_name).await;
        match file {
            Ok(mut file) => {
                file.seek(SeekFrom::Start(data_object.position))
                    .await
                    .unwrap();
                let mut data = vec![0; data_object.length];
                file.read_exact(&mut data).await.unwrap();
                let data_object = bincode::deserialize::<InsertData>(&data);
                match data_object {
                    Ok(data_object) => Ok(data_object),
                    Err(e) => {
                        error!("Error: {:?}", e);
                        Err(SerializeError::Deserialize("Error deserializing data".to_string()))
                    }
                }
            }
            Err(e) => {
                error!("Error: {:?}", e);
                Err(SerializeError::Serialize("Error opening data file".to_string()))
            }
        }
    }

    ///
    /// Update the data object at the given position with the new data
    /// Inactivates the old data object in the old index position and writes the new data to the end of the file then returns the new index position
    async fn update_record(
        &self,
        current_posision: u64,
        current_len: usize,
        update_data: &InsertData,
    ) -> Result<IndexId, SerializeError> {
        let old_index_id = IndexId {
            position: current_posision,
            length: current_len,
        };
        let old_data = self.get_data_object(&old_index_id).await;
        let mut old_data =
            old_data.map_err(|_| SerializeError::Update("Error getting old data".to_string()))?;

        let data = bincode::serialize(update_data)
            .map_err(|_| SerializeError::Update("Error serializing update data".to_string()))?;
        let data_file_name = format!("{}/{}.dat", self.root, self.data_object);
        let file = File::options()
            .append(true)
            .open(data_file_name)
            .await
            .map_err(|er| {
                error!("Error: {:?}", er);
                SerializeError::Serialize("Error opening data file".to_string())
            })?; // Data file
                 // should be available at this point
        let data_len = data.len();
        let (position, file) = self.write_to_end(file, data).await?;

        // Inactivate the old data
        old_data.active = false;
        let old_serialized = bincode::serialize(&old_data)
            .map_err(|_| SerializeError::Update("Error serializing old data".to_string()))?;
        self.seek_and_write(file, position, old_serialized).await?; //#FIXME: We should rollback the data if this fails
        Ok(IndexId {
            position,
            length: data_len,
        })
    }

    async fn delete_record(&mut self, index_id: &IndexId) -> Result<(), SerializeError> {
        let data_file_name = format!("{}/{}.dat", self.root, self.data_object);
        let file = File::open(data_file_name).await;
        match file {
            Ok(mut file) => {
                let mut data = vec![0; index_id.length];
                file.seek(SeekFrom::Start(index_id.position)).await.unwrap();
                file.read_exact(&mut data).await.unwrap();
                let data_object = bincode::deserialize::<InsertData>(&data);
                match data_object {
                    Ok(mut data_object) => {
                        data_object.active = false;
                        let data = bincode::serialize(&data_object).map_err(|_| {
                            SerializeError::Delete("Error serializing data".to_string())
                        })?;
                        self.seek_and_write(file, index_id.position, data).await?;
                        Ok(())
                    }
                    Err(e) => {
                        error!("Error: {:?}", e);
                        Err(SerializeError::Deserialize("Error deserializing data".to_string()))
                    }
                }
            }
            Err(e) => {
                error!("Error: {:?}", e);
                Err(SerializeError::Update(
                    "Error opening data file".to_string(),
                ))
            }
        }
    }

    async fn write_to_end(
        &self,
        mut file: File,
        data: Vec<u8>,
    ) -> Result<(u64, File), SerializeError> {
        let position = file.seek(SeekFrom::End(0)).await.unwrap();
        debug!("Writing data to file: {:?}", position);
        file.write_all(&data).await.unwrap();
        file.flush().await.unwrap();
        Ok((position, file))
    }

    async fn seek_and_write(
        &self,
        mut file: File,
        position: u64,
        data: Vec<u8>,
    ) -> Result<u64, SerializeError> {
        let position = file.seek(SeekFrom::Start(position)).await.unwrap();
        debug!("Writing data to file: {:?}", position);
        file.write_all(&data).await.unwrap();
        file.flush().await.unwrap();
        Ok(position)
    }

    pub async fn seek_and_read(
        &mut self,
        position: u64,
        length: usize,
    ) -> Result<Vec<u8>, SerializeError> {
        let data_file_name = format!("{}/{}.dat", self.root, self.data_object);
        let mut file = File::open(data_file_name).await.unwrap();

        file.seek(SeekFrom::Start(position)).await.unwrap();
        let mut data = vec![0; length];
        file.read_exact(&mut data).await.unwrap();
        Ok(data)
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::parser::{Data, DataObject, InsertData};
    use std::collections::HashMap;
    use tempfile::Builder;

    #[tokio::test]
    async fn test_insert() {
        let dir = Builder::new()
            .prefix("data")
            .tempdir()
            .expect("Failed to create temp directory");
        let data_file_path = dir.path().join("test.dat");
        let _file = File::create(&data_file_path)
            .await
            .expect("Failed to create temp file");

        let root_dir = data_file_path
            .parent()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        let mut nosql_data_object = NoSqlDataObject {
            data_object: "test".to_string(),
            index: HashMap::new(),
            root: root_dir,
        };

        let data = Data {
            key: "name".to_string(),
            value: DataObject::String("123".to_string()),
        };
        let data_object = DataObject::Object(vec![data]);
        let object_id = uuid::Uuid::new_v4().to_string();
        let insert_data = InsertData {
            data_object_id: object_id.clone(),
            table: "test".to_string(),
            data: data_object,
            active: true,
        };
        let index_id = nosql_data_object.insert_record(&insert_data).await;
        let index_id = index_id.unwrap();
        assert_eq!(index_id.length, 96);

        let data = nosql_data_object.get_record(vec![&index_id]).await.unwrap();
        match &data[0].data {
            DataObject::Object(data) => {
                assert_eq!(data[0].key, "name");
                assert_eq!(data[0].value, DataObject::String("123".to_string()));
            }
            _ => panic!("Data not found"),
        }

        let data = Data {
            key: "name".to_string(),
            value: DataObject::String("456".to_string()),
        };

        let data_object = DataObject::Object(vec![data]);
        let update_data = InsertData {
            data_object_id: object_id,
            table: "test".to_string(),
            data: data_object,
            active: true,
        };
        let new_index_id = nosql_data_object
            .update_record(index_id.position, index_id.length, &update_data)
            .await
            .unwrap();
        let data = nosql_data_object
            .get_record(vec![&new_index_id])
            .await
            .unwrap();
        match &data[0].data {
            DataObject::Object(data) => {
                assert_eq!(data[0].key, "name");
                assert_eq!(data[0].value, DataObject::String("456".to_string()));
            }
            _ => panic!("Data not found"),
        }
    }
}
