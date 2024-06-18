use std::{collections::BTreeMap, fmt::Display};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

use crate::data_object::RangeOp;

/// Error type for index operations
#[derive(Debug)]
pub enum IndexError {
    /// Error reading or writing to a file
    FileError(std::io::Error),
    /// Error loading the index from the index file
    Load(String),
    /// Error saving the index to the index file
    Save(String),
}

impl Display for IndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexError::FileError(e) => write!(f, "File Error: {}", e),
            IndexError::Load(e) => write!(f, "Load Error: {}", e),
            IndexError::Save(e) => write!(f, "Save Error: {}", e),
        }
    }
}

/// IndexId is a struct that holds the position and length of an object in the data file.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct IndexId {
    // Position of the object in the data file
    pub position: u64,
    // Length of the object in the data file
    pub length: usize,
}

#[async_trait]
pub trait Index: Send + Sync {
    /// Get the object ids for a given key
    /// # Arguments
    /// * `key` - The key to look up
    /// # Returns
    /// * `Option<Vec<IndexId>>` - The object ids for the given key
    ///
    /// # Example
    /// ```
    /// let index = IndexImpl::new();
    /// let key = "test";
    /// let result = index.get(key);
    /// ```
    fn get(&self, key: &str) -> Option<&Vec<IndexId>>;

    /// Add an object id to the index.
    /// If the key already exists, the object id is appended to the list of object ids. If the key does not exist, a new key is created with the object id.
    /// # Arguments
    /// * `value` - The value to add to the index
    /// * `object_id` - The object id to add to the index value
    /// # Example
    /// ```
    /// let index = IndexImpl::new();
    /// let value = "test";
    /// let object_id = IndexId { position: 0, length: 1 };
    /// index.add_to_index(value, object_id);
    /// ```

    fn add_to_index(&mut self, value: &str, object_id: &IndexId);
    /// Remove an object id from the index. If the key does not exist, nothing happens.
    /// # Arguments
    /// * `value` - The index value.
    /// * `object_id` - The object id to remove from the index value.
    /// # Example
    /// ```
    /// let index = IndexImpl::new();
    /// let value = "test";
    /// let object_id = IndexId { position: 0, length: 1 };
    /// index.remove_from_index(value, object_id);
    /// ```
    fn remove_from_index(&mut self, value: &str, object_id: &IndexId);

    /// Query the index for a value that is equal to the given value. This is an exact match query.
    /// If the value does not exist in the index, an empty vector is returned.
    /// # Arguments
    /// * `value` - The index value to query for
    /// # Returns
    /// * `Vec<&IndexId>` - The object ids for the given index value
    /// # Example
    /// ```
    fn query_equal(&self, value: &str) -> Vec<&IndexId>;

    /// Query the index for a value that is within a given range. The range is determined by the `op` parameter.
    /// If the value does not exist in the index, an empty vector is returned.
    /// # Arguments
    /// * `value` - The index value to query for
    /// * `op` - The range operator to use for the query. `op` can be `RangeOp::GreaterThan`, `RangeOp::GreaterThanOrEqual`, `RangeOp::LessThan`, or `RangeOp::LessThanOrEqual`
    /// # Returns
    /// * `Vec<&IndexId>` - The object ids for the given index value.
    /// # Example
    /// ```
    /// let index = IndexImpl::new();
    /// let value = "test";
    /// let op = RangeOp::GreaterThan;
    /// let result = index.query_range(value, op);
    /// ```

    fn query_range(&self, value: &str, op: RangeOp) -> Vec<&IndexId>;
    /// Query the index for a value that starts with the given prefix. If the value does not exist in the index, an empty vector is returned.
    /// # Arguments
    /// * `prefix` - The prefix to query for
    /// # Returns
    /// * `Vec<&IndexId>` - The object ids for the given index value.
    /// # Example
    /// ```
    /// let index = IndexImpl::new();
    /// let prefix = "test";
    /// let result = index.query_prefix(prefix);
    /// ```
    fn query_prefix(&self, prefix: &str) -> Vec<&IndexId>;

    /// Query the index for a value that ends with the given suffix. If the value does not exist in the index, an empty vector is returned.
    /// # Arguments
    /// * `suffix` - The suffix to query for
    /// # Returns
    /// * `Vec<&IndexId>` - The object ids for the given index value.
    /// # Example
    /// ```
    /// let index = IndexImpl::new();
    /// let suffix = "test";
    /// let result = index.query_suffix(suffix);
    /// ```
    fn query_suffix(&self, suffix: &str) -> Vec<&IndexId>;

    /// Query the index for a value that contains the given substring. If the value does not exist in the index, an empty vector is returned.
    /// # Arguments
    /// * `substring` - The substring to query for
    /// # Returns
    /// * `Vec<&IndexId>` - The object ids for the given index value.
    /// # Example
    /// ```
    /// let index = IndexImpl::new();
    /// let substring = "test";
    /// let result = index.query_contains(substring);
    /// ```
    fn query_contains(&self, substring: &str) -> Vec<&IndexId>;

    /// Save the index to the index file. If an error occurs, an IndexError is returned.
    /// # Returns
    /// * `Result<(), IndexError>` - The result of saving the index
    /// # Example
    /// ```
    /// let index = IndexImpl::new();
    /// let result = index.save();
    /// ```
    async fn save(&mut self) -> Result<(), IndexError>;
}

pub struct IndexImpl {
    index_map: BTreeMap<String, Vec<IndexId>>, // Attribute Value, Object Ids
    index_file: File,                          // File to store the index
}

pub async fn new_or_load(attribute: &str, parent_path: &str) -> Result<Box<dyn Index>, IndexError> {
    let index_file = format!("{}/{}.idx", parent_path, attribute);
    if tokio::fs::metadata(&index_file).await.is_err() {
        // Index file does not exist yet so we create it
        let file = File::create(&index_file).await;
        let _ = file.map_err(|e| IndexError::FileError(e))?;
    }

    match File::options()
        .read(true)
        .write(true)
        .open(&index_file)
        .await
    {
        Ok(mut file) => {
            match file.metadata().await {
                Ok(metadata) => {
                    // If the file is empty, we can return a new IndexManager
                    if metadata.len() == 0 {
                        Ok(Box::new(IndexImpl {
                            index_file: file,
                            index_map: BTreeMap::new(),
                        }))
                    } else {
                        let mut buffer = Vec::new();
                        if let Err(e) = file.read_to_end(&mut buffer).await {
                            return Err(IndexError::Load(format!(
                                "Error reading index file: {}",
                                e
                            )));
                        }

                        let index_map =
                            bincode::deserialize::<BTreeMap<String, Vec<IndexId>>>(&buffer);
                        match index_map {
                            Ok(index_map) => Ok(Box::new(IndexImpl {
                                index_file: file,
                                index_map,
                            })),
                            Err(e) => Err(IndexError::Load(format!(
                                "Error deserializing index file: {}",
                                e
                            ))),
                        }
                    }
                }
                Err(e) => Err(IndexError::Load(format!(
                    "Error reading index file metadata: {}",
                    e
                ))),
            }
        }
        Err(_) => todo!(),
    }
}

#[async_trait]
impl Index for IndexImpl {
    async fn save(&mut self) -> Result<(), IndexError> {
        let serialized = bincode::serialize(&self.index_map.clone());

        match serialized {
            Ok(data) => {
                if let Err(e) = self.index_file.set_len(0).await {
                    return Err(IndexError::Save(format!(
                        "Error truncating index file: {}",
                        e
                    )));
                }

                if let Err(e) = self.index_file.seek(tokio::io::SeekFrom::Start(0)).await {
                    return Err(IndexError::Save(format!("Error seeking index file: {}", e)));
                }
                if let Err(e) = self.index_file.write_all(&data).await {
                    return Err(IndexError::Save(format!("Error writing index file: {}", e)));
                }
                Ok(())
            }
            Err(e) => Err(IndexError::Save(format!("Error serializing index: {}", e))),
        }
    }

    fn get(&self, key: &str) -> Option<&Vec<IndexId>> {
        self.index_map.get(key)
    }

    fn add_to_index(&mut self, value: &str, object_id: &IndexId) {
        self.index_map
            .entry(value.to_string())
            .or_default()
            .push(object_id.clone());
    }

    fn remove_from_index(&mut self, value: &str, object_id: &IndexId) {
        if let Some(object_ids) = self.index_map.get_mut(value) {
            object_ids.retain(|id| id != object_id);
        }
    }

    fn query_equal(&self, value: &str) -> Vec<&IndexId> {
        if let Some(object_ids) = self.index_map.get(value) {
            return object_ids.iter().collect();
        }
        vec![]
    }

    fn query_range(&self, value: &str, op: RangeOp) -> Vec<&IndexId> {
        let mut range = Vec::new();

        for (key, index_id) in &self.index_map {
            match op {
                RangeOp::GreaterThan => {
                    if key > &value.to_string() {
                        range.push(index_id);
                    }
                }
                RangeOp::GreaterThanOrEqual => {
                    if key >= &value.to_string() {
                        range.push(index_id);
                    }
                }
                RangeOp::LessThan => {
                    if key < &value.to_string() {
                        range.push(index_id);
                    }
                }
                RangeOp::LessThanOrEqual => {
                    if key <= &value.to_string() {
                        range.push(index_id);
                    }
                }
            };
        }
        let mut results = Vec::new();
        for object_ids in range {
            results.extend(object_ids);
        }
        results
    }

    fn query_prefix(&self, prefix: &str) -> Vec<&IndexId> {
        let mut results = Vec::new();
        for (_key, object_ids) in self
            .index_map
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
        {
            results.extend(object_ids);
        }
        results
    }

    fn query_suffix(&self, suffix: &str) -> Vec<&IndexId> {
        let mut results = Vec::new();
        for (_key, object_ids) in self.index_map.iter().filter(|(k, _)| k.ends_with(suffix)) {
            results.extend(object_ids);
        }
        results
    }

    fn query_contains(&self, substring: &str) -> Vec<&IndexId> {
        let mut results = Vec::new();
        for (_key, object_ids) in self.index_map.iter().filter(|(k, _)| k.contains(substring)) {
            results.extend(object_ids);
        }
        results
    }
}

#[cfg(test)]
mod test {

    use std::fs;

    use tempfile::Builder;

    use super::*;
    use crate::data_object::RangeOp;

    #[test]
    fn test_add_to_index() {
        let dir = Builder::new()
            .prefix("data")
            .tempdir()
            .expect("Failed to create temp directory");
        let path = dir.path();
        fs::create_dir_all(path).unwrap();
        let root_dir = path.parent().unwrap().to_str().unwrap().to_string();
        let mut index = IndexImpl {
            index_map: BTreeMap::new(),
            index_file: File::from_std(
                std::fs::File::create(format!("{}/test.idx", root_dir)).unwrap(),
            ),
        };
        let test_1_index_id = IndexId {
            position: 0,
            length: 1,
        };
        index.add_to_index("test", &test_1_index_id);
        let object_ids = index.index_map.get("test").unwrap();
        assert_eq!(object_ids.len(), 1);
        assert_eq!(object_ids[0], test_1_index_id);
        let test_2_index_id = IndexId {
            position: 1,
            length: 1,
        };
        index.add_to_index("test", &test_2_index_id);
        let object_ids = index.index_map.get("test").unwrap();
        assert_eq!(object_ids.len(), 2);
        assert_eq!(object_ids[0], test_1_index_id);
        assert_eq!(object_ids[1], test_2_index_id);
    }

    #[test]
    fn test_remove_from_index() {
        let dir = Builder::new()
            .prefix("data")
            .tempdir()
            .expect("Failed to create temp directory");
        let path = dir.path();
        fs::create_dir_all(path).unwrap();
        let root_dir = path.parent().unwrap().to_str().unwrap().to_string();
        let mut index = IndexImpl {
            index_map: BTreeMap::new(),
            index_file: File::from_std(
                std::fs::File::create(format!("{}/test.idx", root_dir)).unwrap(),
            ),
        };
        let test_1_index_id = IndexId {
            position: 0,
            length: 1,
        };
        index.add_to_index("test", &test_1_index_id);
        let test_2_index_id = IndexId {
            position: 1,
            length: 1,
        };
        index.add_to_index("test", &test_2_index_id);
        index.remove_from_index("test", &test_1_index_id);
        let object_ids = index.index_map.get("test").unwrap();
        assert_eq!(object_ids.len(), 1);
        assert_eq!(object_ids[0], test_2_index_id);
        index.remove_from_index("test", &test_2_index_id);
        let object_ids = index.index_map.get("test");
        assert_eq!(object_ids.unwrap().len(), 0);
    }

    #[test]
    fn test_query_equal() {
        let dir = Builder::new()
            .prefix("data")
            .tempdir()
            .expect("Failed to create temp directory");

        let path = dir.path();
        fs::create_dir_all(path).unwrap();

        let root_dir = path.parent().unwrap().to_str().unwrap().to_string();

        let mut index = IndexImpl {
            index_map: BTreeMap::new(),
            index_file: File::from_std(
                std::fs::File::create(format!("{}/test.idx", root_dir)).unwrap(),
            ),
        };

        let test_1_index_id = IndexId {
            position: 0,
            length: 1,
        };

        index.add_to_index("test", &test_1_index_id);
        index.add_to_index("test", &test_1_index_id);

        let test_2_index_id = IndexId {
            position: 1,
            length: 1,
        };

        index.add_to_index("test2", &test_2_index_id);

        let result = index.query_equal("test");
        assert_eq!(result.len(), 2);
        let result = index.query_equal("test2");
        assert_eq!(result.len(), 1);
        let result = index.query_equal("test3");
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_start_with() {
        let dir = Builder::new()
            .prefix("data")
            .tempdir()
            .expect("Failed to create temp directory");

        let path = dir.path();
        fs::create_dir_all(path).unwrap();

        let root_dir = path.parent().unwrap().to_str().unwrap().to_string();

        let mut index = IndexImpl {
            index_map: BTreeMap::new(),
            index_file: File::from_std(
                std::fs::File::create(format!("{}/test.idx", root_dir)).unwrap(),
            ),
        };

        let test_1_index_id = IndexId {
            position: 0,
            length: 1,
        };

        index.add_to_index("atest", &test_1_index_id);
        index.add_to_index("atestX", &test_1_index_id);

        let test_2_index_id = IndexId {
            position: 1,
            length: 1,
        };

        index.add_to_index("another1", &test_2_index_id);

        let result = index.query_prefix("atest");
        assert_eq!(result.len(), 2);
        let result = index.query_prefix("test");
        assert_eq!(result.len(), 0);
        let result = index.query_prefix("a");
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_ends_with() {
        let dir = Builder::new()
            .prefix("data")
            .tempdir()
            .expect("Failed to create temp directory");

        let path = dir.path();
        fs::create_dir_all(path).unwrap();

        let root_dir = path.parent().unwrap().to_str().unwrap().to_string();

        let mut index = IndexImpl {
            index_map: BTreeMap::new(),
            index_file: File::from_std(
                std::fs::File::create(format!("{}/test.idx", root_dir)).unwrap(),
            ),
        };

        let test_1_index_id = IndexId {
            position: 0,
            length: 1,
        };

        index.add_to_index("test1", &test_1_index_id);
        index.add_to_index("test2", &test_1_index_id);

        let test_2_index_id = IndexId {
            position: 1,
            length: 1,
        };

        index.add_to_index("another1", &test_2_index_id);

        let result = index.query_suffix("1");
        assert_eq!(result.len(), 2);
        let result = index.query_suffix("2");
        assert_eq!(result.len(), 1);
        let result = index.query_suffix("3");
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_contains() {
        let dir = Builder::new()
            .prefix("data")
            .tempdir()
            .expect("Failed to create temp directory");

        let path = dir.path();
        fs::create_dir_all(path).unwrap();

        let root_dir = path.parent().unwrap().to_str().unwrap().to_string();

        let mut index = IndexImpl {
            index_map: BTreeMap::new(),
            index_file: File::from_std(
                std::fs::File::create(format!("{}/test.idx", root_dir)).unwrap(),
            ),
        };

        let test_1_index_id = IndexId {
            position: 0,
            length: 1,
        };

        index.add_to_index("test1", &test_1_index_id);
        index.add_to_index("test2", &test_1_index_id);

        let test_2_index_id = IndexId {
            position: 1,
            length: 1,
        };

        index.add_to_index("another1", &test_2_index_id);

        let result = index.query_contains("est");
        assert_eq!(result.len(), 2);
        let result = index.query_contains("t");
        assert_eq!(result.len(), 3);
        let result = index.query_contains("not");
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_range() {
        let dir = Builder::new()
            .prefix("data")
            .tempdir()
            .expect("Failed to create temp directory");

        let path = dir.path();
        fs::create_dir_all(path).unwrap();

        let root_dir = path.parent().unwrap().to_str().unwrap().to_string();

        let mut index = IndexImpl {
            index_map: BTreeMap::new(),
            index_file: File::from_std(
                std::fs::File::create(format!("{}/test.idx", root_dir)).unwrap(),
            ),
        };

        let test_1_index_id = IndexId {
            position: 0,
            length: 1,
        };

        index.add_to_index("1", &test_1_index_id);
        index.add_to_index("2", &test_1_index_id);

        let test_2_index_id = IndexId {
            position: 1,
            length: 1,
        };

        index.add_to_index("4", &test_2_index_id);

        let result = index.query_range("2", RangeOp::GreaterThan);
        assert_eq!(result.len(), 1);
        let result = index.query_range("2", RangeOp::GreaterThanOrEqual);
        assert_eq!(result.len(), 2);
        let result = index.query_range("2", RangeOp::LessThan);
        assert_eq!(result.len(), 1);
        let result = index.query_range("2", RangeOp::LessThanOrEqual);
        assert_eq!(result.len(), 2);
    }

    #[tokio::test]
    async fn test_save_load() {
        let dir = Builder::new()
            .prefix("data")
            .tempdir()
            .expect("Failed to create temp directory");
        let path = dir.path();
        fs::create_dir_all(path).unwrap();
        let root_dir = path.parent().unwrap().to_str().unwrap().to_string();
        let mut index = IndexImpl {
            index_map: BTreeMap::new(),
            index_file: File::from_std(
                std::fs::File::create(format!("{}/test.idx", root_dir)).unwrap(),
            ),
        };
        let test_1_index_id = IndexId {
            position: 0,
            length: 1,
        };
        index.add_to_index("test1", &test_1_index_id);
        index.add_to_index("tes2", &test_1_index_id);
        let test_2_index_id = IndexId {
            position: 1,
            length: 1,
        };
        index.add_to_index("test1", &test_2_index_id);
        let result = index.save().await;
        assert!(result.is_ok());

        let index = new_or_load("test", &root_dir).await;
        assert!(index.is_ok());
        let index = index.unwrap();

        let object_id = index.get("test1");
        assert!(object_id.is_some());
        let object_id = object_id.unwrap();
        assert_eq!(object_id.len(), 2);
        assert_eq!(object_id[0], test_1_index_id);
        assert_eq!(object_id[1], test_2_index_id);

        let object_id = index.get("tes2");
        assert!(object_id.is_some());
        let object_id = object_id.unwrap();
        assert_eq!(object_id.len(), 1);
        assert_eq!(object_id[0], test_1_index_id);

        let object_id = index.get("test3");
        assert!(object_id.is_none());
    }
}
