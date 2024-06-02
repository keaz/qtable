use std::collections::HashMap;

use crate::{
    data_object::NoSqlDataObject,
    index::IndexId,
    parser::{handle_message, Query},
};

pub struct NoSqlDatabase {
    data_objects: HashMap<String, NoSqlDataObject>,
    data_base: String,
}

impl NoSqlDatabase {
    pub async fn handle_message(&mut self, message: &str) {
        let message = handle_message(&self.data_base, message);
        match message {
            Ok(message) => match message {
                crate::parser::Command::Select(query) => {
                    let object_ids = self.handle_query(query);
                }
                crate::parser::Command::Insert(_) => todo!(),
                crate::parser::Command::Update(_) => todo!(),
                crate::parser::Command::Delete(_) => todo!(),
                crate::parser::Command::Create => todo!(),
                crate::parser::Command::Define(_, _) => todo!(),
                crate::parser::Command::Alter => todo!(),
                crate::parser::Command::Drop => todo!(),
            },
            Err(e) => {}
        }
    }

    pub fn handle_query(&self, query: Query) -> Vec<&IndexId> {
        if let Some(data_object) = self.data_objects.get(&query.table_name) {
            return data_object.query(&query.filter);
        }
        return vec![]; // this should be an error
    }
}
