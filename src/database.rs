use std::collections::HashMap;

use crate::{data_object::NoSqlDataObject, index::ObjectId, parser::Query};

pub struct NoSqlDatabase {
    data_objects: HashMap<String, NoSqlDataObject>,
}

impl NoSqlDatabase {
    pub fn query(&self, query: Query) -> Vec<&ObjectId> {
        if let Some(data_object) = self.data_objects.get(&query.table_name) {
            return data_object.query(&query.filter);
        }
        return vec![]; // this should be an error
    }
}
