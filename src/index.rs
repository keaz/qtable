use std::{
    collections::{btree_map::Range, BTreeMap},
    fmt::Display,
    ops::RangeBounds,
};

use serde::{Deserialize, Serialize};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

use crate::{data_object::RangeOp, parser::WildCardOperations};

pub enum IndexError {
    FileError(std::io::Error),
    Load(String),
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

#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub struct IndexId {
    pub position: u64,
    pub length: usize,
}

pub struct Index {
    index_map: BTreeMap<String, Vec<IndexId>>, // Attribute Value, Object Ids
    index_file: File,                          // File to store the index
}

impl Index {
    pub async fn new_or_load(attribute: &str, parent_path: &str) -> Result<Self, IndexError> {
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
                            Ok(Index {
                                index_file: file,
                                index_map: BTreeMap::new(),
                            })
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
                                Ok(index_map) => Ok(Index {
                                    index_file: file,
                                    index_map,
                                }),
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

    pub async fn save(&mut self) -> Result<(), IndexError> {
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

    pub fn get(&self, key: &str) -> Option<&Vec<IndexId>> {
        self.index_map.get(key)
    }

    pub fn range<R>(&self, range: R) -> Range<'_, String, Vec<IndexId>>
    where
        R: RangeBounds<String>,
    {
        self.index_map.range(range)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Vec<IndexId>)> {
        self.index_map.iter()
    }

    pub fn add_to_index(&mut self, value: &str, object_id: &IndexId) {
        self.index_map
            .entry(value.to_string())
            .or_default()
            .push(object_id.clone());
    }

    pub fn remove_from_index(&mut self, value: &str, object_id: &IndexId) {
        if let Some(object_ids) = self.index_map.get_mut(value) {
            object_ids.retain(|id| id != object_id);
        }
    }

    fn query_wildcard(&self, op: &WildCardOperations) -> Vec<&IndexId> {
        match op {
            WildCardOperations::StartsWith(_attr, prefix) => self.query_prefix(prefix),
            WildCardOperations::EndsWith(_attr, suffix) => self.query_suffix(suffix),
            WildCardOperations::Contains(_attr, substring) => self.query_contains(substring),
        }
    }

    pub fn query_equal(&self, value: &str) -> Vec<&IndexId> {
        if let Some(object_ids) = self.index_map.get(value) {
            return object_ids.iter().collect();
        }
        vec![]
    }

    pub fn query_range(&self, value: &str, op: RangeOp) -> Vec<&IndexId> {
        let range = match op {
            RangeOp::GreaterThan => self.index_map.range((value.to_string())..),
            RangeOp::GreaterThanOrEqual => self.index_map.range(value.to_string()..),
            RangeOp::LessThan => self.index_map.range(..value.to_string()),
            RangeOp::LessThanOrEqual => self.index_map.range(..=(value.to_string())),
        };
        let mut results = Vec::new();
        for (_key, object_ids) in range {
            results.extend(object_ids);
        }
        results
    }

    pub fn query_prefix(&self, prefix: &str) -> Vec<&IndexId> {
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

    pub fn query_suffix(&self, suffix: &str) -> Vec<&IndexId> {
        let mut results = Vec::new();
        for (_key, object_ids) in self.index_map.iter().filter(|(k, _)| k.ends_with(suffix)) {
            results.extend(object_ids);
        }
        results
    }

    pub fn query_contains(&self, substring: &str) -> Vec<&IndexId> {
        let mut results = Vec::new();
        for (_key, object_ids) in self.index_map.iter().filter(|(k, _)| k.contains(substring)) {
            results.extend(object_ids);
        }
        results
    }
}
