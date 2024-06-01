use std::{collections::HashMap, io::SeekFrom};

use log::{debug, error};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

use crate::{
    index::{Index, ObjectId},
    parser::{Condition, InsertData, UpdateData, WildCardOperations},
};

pub struct NoSqlDataObject {
    data_object: String,
    index: HashMap<String, Index>,
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
    SerializeError,
    DeserializeError,
}

impl NoSqlDataObject {
    pub fn add_to_index(&mut self, attribute: &str, value: &str, object_id: &ObjectId) {
        if let Some(index) = self.index.get_mut(attribute) {
            index.add_to_index(value, object_id);
        }
    }
    pub fn query(&self, condition: &Condition) -> Vec<&ObjectId> {
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

    fn query_wildcard(&self, op: &WildCardOperations) -> Vec<&ObjectId> {
        match op {
            WildCardOperations::StartsWith(attr, prefix) => self.query_prefix(attr, prefix),
            WildCardOperations::EndsWith(attr, suffix) => self.query_suffix(attr, suffix),
            WildCardOperations::Contains(attr, substring) => self.query_contains(attr, substring),
        }
    }

    fn query_equal(&self, attr: &str, value: &str) -> Vec<&ObjectId> {
        if let Some(index) = self.index.get(attr) {
            return index.query_equal(value);
        }
        vec![]
    }

    fn query_range(&self, attr: &str, value: &str, op: RangeOp) -> Vec<&ObjectId> {
        if let Some(index) = self.index.get(attr) {
            return index.query_range(value, op);
        }
        vec![]
    }

    fn query_prefix(&self, attr: &str, prefix: &str) -> Vec<&ObjectId> {
        if let Some(index) = self.index.get(attr) {
            return index.query_prefix(prefix);
        }
        vec![]
    }

    fn query_suffix(&self, attr: &str, suffix: &str) -> Vec<&ObjectId> {
        if let Some(index) = self.index.get(attr) {
            return index.query_suffix(suffix);
        }
        vec![]
    }

    fn query_contains(&self, attr: &str, substring: &str) -> Vec<&ObjectId> {
        if let Some(index) = self.index.get(attr) {
            return index.query_contains(substring);
        }
        vec![]
    }
}

impl NoSqlDataObject {
    pub async fn insert(&mut self, insert_data: InsertData) -> Result<ObjectId, SerializeError> {
        let serialized = bincode::serialize(&insert_data.data);
        match serialized {
            Ok(data) => {
                let data_file_name = format!("{}/{}.dat", self.root, self.data_object);
                let file = File::options().append(true).open(data_file_name).await; // Data file
                                                                                    // should be available at this point
                match file {
                    Ok(file) => {
                        let data = format!("{}\n", String::from_utf8(data).unwrap());
                        let data_len = data.len();
                        let position = self.seek_and_write(file, data.as_bytes().to_vec()).await?;
                        Ok(ObjectId {
                            position,
                            length: data_len,
                        })
                    }
                    Err(e) => {
                        error!("Error: {:?}", e);
                        Err(SerializeError::SerializeError)
                    }
                }
            }
            Err(e) => {
                error!("Error: {:?}", e);
                Err(SerializeError::SerializeError)
            }
        }
    }

    pub async fn get_data(
        &self,
        data_objects: Vec<&ObjectId>,
    ) -> Result<Vec<Vec<u8>>, SerializeError> {
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
                    data.push(data_chunk);
                }
            }
            Err(e) => {
                error!("Error: {:?}", e);
                return Err(SerializeError::SerializeError);
            }
        }
        Ok(data)
    }

    // TODO Update function
    pub async fn update(&self, current_posision: u64, current_len: usize, update_data: UpdateData) {
    }

    async fn seek_and_write(&self, mut file: File, data: Vec<u8>) -> Result<u64, SerializeError> {
        let position = file.seek(SeekFrom::End(0)).await.unwrap();
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
