use std::{
    collections::HashMap,
    fmt::{format, Display},
};

use log::error;
use nom::{
    branch::alt,
    bytes::complete::{tag, take_while, take_while1},
    character::complete::{alpha1, char, multispace0, multispace1, space0},
    combinator::map,
    multi::many0,
    sequence::{delimited, preceded, tuple},
    IResult,
};
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};

// DML
const SELECT: &str = "SELECT";
const INSERT: &str = "INSERT";
const UPDATE: &str = "UPDATE";
const DELETE: &str = "DELETE";

// DDL
const CREATE: &str = "CREATE";
const DEFINE: &str = "DEFINE"; // create structure
const ALTER: &str = "ALTER";
const DROP: &str = "DROP";

/// Data type for the database
///
/// # Example
///
/// ```
/// use crate::parse::{DataType, Data};
///
/// let data = DataType::Object(vec![Data {
///    key: "test".to_string(),
///   value: DataType::String("test".to_string()),
/// }]);
/// ```
#[derive(Serialize, Deserialize, Debug)]
pub enum DataObject {
    String(String),
    Number(Number),
    Bool(bool),
    Array(Vec<DataObject>),
    Object(Vec<Data>),
}

/// Data is a struct that represents a key value pair
///
/// # Example
///
/// ```
/// use crate::parse::{Data, DataType};
///
/// let data = Data {
///     key: "test".to_string(),
///     value: DataType::String("test".to_string()),
/// };
/// ```
#[derive(Serialize, Deserialize, Debug)]
pub struct Data {
    pub key: String,
    pub value: DataObject,
}

#[derive(Debug)]
pub enum SyntaxErrorCode {
    UnKnownKeyWord,
    InvalidOperator,
    UnKnownOperator,
    InvalidDefinition,
    InvalidDataType,
    InvalidValue,
}

impl Display for SyntaxErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyntaxErrorCode::UnKnownKeyWord => {
                write!(f, "1000: Unknown keyword")
            }
            SyntaxErrorCode::InvalidOperator => {
                write!(f, "1001: Invalid operator")
            }
            SyntaxErrorCode::UnKnownOperator => {
                write!(f, "1002: Unknown operator")
            }
            SyntaxErrorCode::InvalidDefinition => {
                write!(f, "1003: Invalid definition")
            }
            SyntaxErrorCode::InvalidDataType => {
                write!(f, "1004: Invalid data type")
            }
            SyntaxErrorCode::InvalidValue => {
                write!(f, "1005: Invalid value")
            }
        }
    }
}

/// SyntaxError is an enum that represents a syntax error
#[derive(Debug)]
pub enum SyntaxError {
    /// SyntaxError is a variant that represents a syntax error
    SyntaxError(SyntaxErrorCode, String),
    /// ParseError is a variant that represents a parse error
    ParseError(String),
}

/// DeleteData is a type alias for a tuple that represents the data to be deleted
#[derive(Debug)]
pub struct DeleteData {
    pub db: String,
    pub table: String,
    pub data: DataObject,
}

#[derive(Serialize, Deserialize)]
pub struct Define {
    pub column: String,
    pub definition: Definition,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Definition {
    #[serde(rename(serialize = "type"))]
    pub data_type: String,
    pub indexed: bool,
    pub optional: bool,
}

#[derive(Debug)]
pub enum Condition {
    WildCard(WildCardOperations),
    Equal(String, String),
    GreaterThan(String, String),
    GreaterThanOrEqual(String, String),
    LessThan(String, String),
    LessThanOrEqual(String, String),
    And(Box<Condition>, Box<Condition>),
    Or(Box<Condition>, Box<Condition>),
}

#[derive(Debug)]
pub enum WildCardOperations {
    StartsWith(String, String),
    EndsWith(String, String),
    Contains(String, String),
}

#[derive(Debug)]
pub enum FilterValue {
    String(String, String),
    Float(String, f64),
    Int(String, i64),
}

#[derive(Debug)]
pub struct Query {
    pub db: String,
    pub table_name: String,
    pub filter: Condition,
}

#[derive(Debug)]
pub struct InsertData {
    pub data_object_id: String,
    pub table: String,
    pub data: DataObject,
}

#[derive(Debug)]
pub struct UpdateData {
    pub db: String,
    pub table: String,
    pub data: DataObject,
}

/// Command is an enum that represents a command
#[derive(Debug)]
pub enum Command {
    /// Select is a variant that represents a select command
    Select(Query),
    /// Insert is a variant that represents an insert command
    Insert(InsertData),
    Update(UpdateData),
    Delete(DeleteData),
    Create,
    Define(String, HashMap<String, Definition>),
    Alter,
    Drop,
}

/// handle_message is a function that handles a message and returns a Command or a SyntaxError
///
/// # Example
///
/// ```
/// use crate::parse::{handle_message, Command, SyntaxError};
///
/// let message = "SELECT name, age FROM user";
/// let db = "db";
/// let result = handle_message(db,message);
/// match result {
///    Ok(Command::Select(fields, table)) => {
///       assert_eq!(fields, vec!["name", "age"]);
///       assert_eq!(table, "user");
///   }
///  _ => panic!("Expected Select command"),
/// }
/// ```
pub fn handle_message(db: &str, message: &str) -> Result<Command, SyntaxError> {
    let message = message.trim();

    if message.starts_with(SELECT) {
        parse_select(db, message)
    } else if message.starts_with(INSERT) {
        parse_insert_command(db, message)
    } else if message.starts_with(UPDATE) {
        parse_update_command(db, message)
    } else if message.starts_with(DELETE) {
        parse_delete_command(db, message)
    } else if message.starts_with(CREATE) {
        todo!("Create command");
    } else if message.starts_with(DEFINE) {
        parse_define_command(message)
    } else if message.starts_with(ALTER) {
        todo!("Alter command");
    } else if message.starts_with(DROP) {
        todo!("Drop command");
    } else {
        Err(SyntaxError::ParseError(format!(
            "Unknown command: {}",
            message
        )))
    }
}

fn extract_table_name(input: &str) -> IResult<&str, &str> {
    alpha1(input)
}

fn extract_json(input: &str) -> IResult<&str, &str> {
    multispace1(input)
}

fn remove<'a>(input: &'a str, to_remove: &'a str) -> IResult<&'a str, &'a str> {
    let (input, _) = tag(to_remove)(input)?;
    multispace1(input)
}

/// parse_define_command is a function that parses a define command and returns the document structure as a Command or a SyntaxError
/// # Example
/// ```
/// use crate::parse::{parse_define_command, Command, SyntaxError};
/// let message = r#"DEFINE user { name: { type: "String", indexed: true, optional: false }, age: { type: "Number", indexed: false, optional: true}}";
/// let result = parse_define_command(message);
/// match result {
///   Ok(Command::Define(define)) => {
///     assert_eq!(define.len(), 2);
///     assert_eq!(define.get("name").unwrap().column, "name");
///     assert_eq!(define.get("name").unwrap().definition.data_type, "String");
///     assert_eq!(define.get("name").unwrap().definition.indexed, true);
///     assert_eq!(define.get("name").unwrap().definition.optional, false);
///   }
///   _ => panic!("Expected Define command"),
/// }
/// ```
fn parse_define_command(input: &str) -> Result<Command, SyntaxError> {
    let input = match remove(input, DEFINE) {
        Ok((input, _)) => input,
        Err(err) => {
            error!("Error: {:?}", err);
            return Err(SyntaxError::SyntaxError(
                SyntaxErrorCode::UnKnownKeyWord,
                err.to_string(),
            ));
        }
    };

    let (input, table_name) = match extract_table_name(input) {
        Ok((input, table_name)) => (input, table_name),
        Err(err) => {
            error!("Error: {:?}", err);
            return Err(SyntaxError::SyntaxError(
                SyntaxErrorCode::UnKnownKeyWord,
                err.to_string(),
            ));
        }
    };

    let (_, json_str) = match extract_json(input) {
        Ok((json_str, input)) => (input, json_str),
        Err(err) => {
            error!("Error: {:?}", err);
            return Err(SyntaxError::SyntaxError(
                SyntaxErrorCode::InvalidDefinition,
                err.to_string(),
            ));
        }
    };

    let json: Result<Value, serde_json::Error> = serde_json::from_str(json_str);

    match json {
        Ok(json) => match json {
            Value::Object(obj) => {
                let mut define = HashMap::new();
                for (key, value) in obj.iter() {
                    let definition = match value {
                        Value::Object(o) => {
                            let data_type = match o.get("type") {
                                Some(Value::String(s)) => s.to_string(),
                                _ => {
                                    return Err(SyntaxError::SyntaxError(
                                        SyntaxErrorCode::InvalidDataType,
                                        format!(
                                            "Invalid value for type, expected String but found {}",
                                            key
                                        ),
                                    ))
                                }
                            };
                            let indexed = match o.get("indexed") {
                                Some(Value::Bool(b)) => *b,
                                _ => {
                                    return Err(SyntaxError::SyntaxError(
                                        SyntaxErrorCode::InvalidValue,
                                        format!(
                                            "Invalid value for indexed, expected Bool but found {}",
                                            key
                                        ),
                                    ))
                                }
                            };
                            let optional = match o.get("optional") {
                                Some(Value::Bool(b)) => *b,
                                _ => {
                                    return Err(SyntaxError::SyntaxError(
                                        SyntaxErrorCode::InvalidValue,
                                        format!(
                                        "Invalid value for optional, expected Bool but found {}",
                                        key
                                    ),
                                    ))
                                }
                            };
                            Definition {
                                data_type,
                                indexed,
                                optional,
                            }
                        }
                        _ => {
                            return Err(SyntaxError::SyntaxError(
                                SyntaxErrorCode::UnKnownKeyWord,
                                format!("Unknown key {}", key),
                            ))
                        }
                    };
                    define.insert(key.to_string(), definition);
                }
                Ok(Command::Define(table_name.to_string(), define))
            }
            _ => Err(SyntaxError::SyntaxError(
                SyntaxErrorCode::InvalidValue,
                format!("Expected Object for definition but found {}", json),
            )),
        },
        Err(e) => {
            error!("Wrong JSON format for define command {:?}", e);
            Err(SyntaxError::SyntaxError(
                SyntaxErrorCode::InvalidValue,
                format!("Wrong JSON format for define command {:?}", e),
            ))
        }
    }
}

fn parse_update_command(db: &str, input: &str) -> Result<Command, SyntaxError> {
    let input = match remove(input, UPDATE) {
        Ok((input, _)) => input,
        Err(err) => {
            error!("Error: {:?}", err);
            return Err(SyntaxError::SyntaxError(
                SyntaxErrorCode::InvalidValue,
                format!("{}", err),
            ));
        }
    };

    let (input, table_name) = match extract_table_name(input) {
        Ok((input, table_name)) => (input, table_name),
        Err(err) => {
            error!("Error: {:?}", err);
            return Err(SyntaxError::ParseError(format!(
                "Could not parse table name: {:?}",
                err
            )));
        }
    };

    let (_, json_str) = match extract_json(input) {
        Ok((input, json_str)) => (input, json_str),
        Err(err) => {
            error!("Error: {:?}", err);
            return Err(SyntaxError::ParseError(format!(
                "Could not parse JSON: {:?}",
                err
            )));
        }
    };

    match parse_json(json_str, table_name) {
        Ok((_id, table, data)) => {
            let update_data = UpdateData {
                db: "".to_string(),
                table: table.to_string(),
                data,
            };
            Ok(Command::Update(update_data))
        }
        Err(e) => {
            error!("Error: {:?}", e);
            Err(SyntaxError::ParseError(format!(
                "Could not parse JSON: {:?}",
                e
            )))
        }
    }
}

/// parse_delete_command is a function that parses a delete command and returns a Command or a SyntaxError
/// # Example
/// ```
/// use crate::parse::{parse_delete_command, Command, SyntaxError};
/// let message = r#"DELETE FROM user { id: "123"}";
/// let result = parse_delete_command(message);
/// match result {
///  Ok(Command::Delete((table, data))) => {
///    assert_eq!(table, "user");
///   match data {
///    DataType::Object(data) => {
///     assert_eq!(data[0].key, "id");
///    match &data[0].value {
///    DataType::String(s) => {
///    assert_eq!(s.as_str(), "123")
///   }
///  _ => panic!("Expected string"),
/// };
/// }
/// _ => panic!("Expected object"),
/// }
/// }
/// _ => panic!("Expected Delete command"),
/// }
/// ```
fn parse_delete_command(db: &str, input: &str) -> Result<Command, SyntaxError> {
    let input = match remove(input, "DELETE FROM") {
        Ok((input, _)) => input,
        Err(err) => {
            error!("Error: {:?}", err);
            return Err(SyntaxError::SyntaxError(
                SyntaxErrorCode::InvalidValue,
                format!("Expected DELETE FROM but found {}", err),
            ));
        }
    };

    let (input, table_name) = match extract_table_name(input) {
        Ok((input, table_name)) => (input, table_name),
        Err(err) => {
            error!("Error: {:?}", err);
            return Err(SyntaxError::ParseError(format!(
                "Could not parse table name: {:?}",
                err
            )));
        }
    };

    let (_, json_str) = match extract_json(input) {
        Ok((json_str, input)) => (input, json_str),
        Err(err) => {
            error!("Error: {:?}", err);
            return Err(SyntaxError::ParseError(format!(
                "Could not parse JSON: {:?}",
                err
            )));
        }
    };

    let (table, data) = parse_delete_json(json_str, table_name)?;
    let delete_data = DeleteData {
        db: db.to_string(),
        table,
        data,
    };
    Ok(Command::Delete(delete_data))
}

fn parse_delete_json<'a>(
    json_str: &'a str,
    table_name: &'a str,
) -> Result<(String, DataObject), SyntaxError> {
    match serde_json::from_str(json_str) {
        Ok(json) => match json {
            Value::Object(obj) => {
                let data = handle_object(obj.to_owned());
                Ok((table_name.to_owned(), data))
            }
            _ => Err(SyntaxError::ParseError(format!(
                "Expected Object but found {}",
                json_str
            ))),
        },
        Err(e) => {
            error!("Error parsing JSON: {}", e);
            Err(SyntaxError::ParseError(format!(
                "Could not parse JSON: {:?}",
                e
            )))
        }
    }
}

fn parse_insert_command(db: &str, input: &str) -> Result<Command, SyntaxError> {
    let input = match remove(input, "INSERT INTO") {
        Ok((input, _)) => input,
        Err(err) => {
            error!("Error: {:?}", err);
            return Err(SyntaxError::SyntaxError(
                SyntaxErrorCode::InvalidValue,
                format!("Expected INSERT INTO but found {}", err),
            ));
        }
    };

    let (input, table_name) = match extract_table_name(input) {
        Ok((input, table_name)) => (input, table_name),
        Err(err) => {
            error!("Error: {:?}", err);
            return Err(SyntaxError::ParseError(format!(
                "Could not parse table name: {:?}",
                err
            )));
        }
    };

    let (_, json_str) = match extract_json(input) {
        Ok((json_str, input)) => (input, json_str),
        Err(err) => {
            error!("Error: {:?}", err);
            return Err(SyntaxError::ParseError(format!(
                "Could not parse JSON: {:?}",
                err
            )));
        }
    };

    let (_id, table, data) = parse_json(json_str, table_name)?;
    let insert_data = InsertData {
        data_object_id: db.to_string(),
        table: table.to_string(),
        data,
    };
    Ok(Command::Insert(insert_data))
}

fn parse_json<'a>(
    json_str: &'a str,
    table_name: &'a str,
) -> Result<(String, String, DataObject), SyntaxError> {
    match serde_json::from_str(json_str) {
        Ok(json) => match json {
            Value::Object(obj) => {
                let id = get_id(&obj)?;
                let data = handle_object(obj.to_owned());
                Ok((id, table_name.to_owned(), data))
            }
            _ => Err(SyntaxError::ParseError(format!(
                "Expected Object but found {}",
                json_str
            ))),
        },
        Err(e) => {
            error!("Error parsing JSON: {}", e);
            Err(SyntaxError::ParseError(format!(
                "Could not parse JSON: {:?}",
                e
            )))
        }
    }
}

fn get_id(obj: &serde_json::Map<String, Value>) -> Result<String, SyntaxError> {
    let id = match obj.get("id") {
        Some(some) => match some {
            Value::String(s) => s.to_owned(),
            _ => {
                return Err(SyntaxError::SyntaxError(
                    SyntaxErrorCode::InvalidValue,
                    "Expected String for ID but found something else".to_string(),
                ))
            }
        },
        None => uuid::Uuid::new_v4().to_string(),
    };
    Ok(id)
}

fn handle_array(array: Vec<Value>) -> DataObject {
    let mut data = Vec::new();
    for value in array {
        match value {
            Value::String(s) => data.push(DataObject::String(s)),
            Value::Number(n) => data.push(DataObject::Number(n)),
            Value::Array(a) => data.push(handle_array(a)),
            Value::Object(o) => data.push(handle_object(o)),
            Value::Bool(b) => data.push(DataObject::Bool(b)),
            Value::Null => (),
        }
    }
    DataObject::Array(data)
}

fn handle_object(object: serde_json::Map<String, Value>) -> DataObject {
    let mut data = Vec::new();
    for (key, value) in object.iter() {
        match value {
            Value::String(s) => data.push(Data {
                key: key.to_string(),
                value: DataObject::String(s.to_string()),
            }),
            Value::Number(n) => data.push(Data {
                key: key.to_string(),
                value: DataObject::Number(n.clone()),
            }),
            Value::Array(a) => data.push(Data {
                key: key.to_string(),
                value: handle_array(a.to_vec()),
            }),
            Value::Object(o) => data.push(Data {
                key: key.to_string(),
                value: handle_object(o.to_owned()),
            }),
            Value::Bool(b) => data.push(Data {
                key: key.to_string(),
                value: DataObject::Bool(*b),
            }),
            Value::Null => (),
        }
    }
    DataObject::Object(data)
}

///
/// parse_select is a function that parses a select command and returns a Command or a SyntaxError
/// # Example
/// ```
/// use crate::parse::{parse_select, Command, SyntaxError};
/// let message = "SELECT db.user WHERE id = '123' and name = 'John' and age >= 30";
/// let result = parse_select(message);
/// match result {
///     Ok(Command::Select(fields, table)) => {
///         assert_eq!(fields, vec!["name", "age"]);
///         assert_eq!(table, "user");
///     }
///     _ => panic!("Expected Select command"),
/// }
/// ```
fn parse_select(db: &str, input: &str) -> Result<Command, SyntaxError> {
    let input = match remove(input, "SELECT") {
        Ok((input, _)) => input,
        Err(err) => {
            error!("Error: {:?}", err);
            return Err(SyntaxError::SyntaxError(
                SyntaxErrorCode::InvalidValue,
                format!("Expected SELECT but found {}", err),
            ));
        }
    };

    let (input, table_name) = match extract_select_table(input) {
        Ok((input, table_name)) => (input, table_name),
        Err(err) => {
            error!("Error: {:?}", err);
            return Err(SyntaxError::ParseError(format!(
                "Could not parse table name: {:?}",
                err
            )));
        }
    };

    let input = match remove(input, "WHERE") {
        Ok((input, _)) => input,
        Err(err) => {
            error!("Error: {:?}", err);
            return Err(SyntaxError::SyntaxError(
                SyntaxErrorCode::InvalidValue,
                format!("Expected WHERE but found {}", err),
            ));
        }
    };

    let (_input, filter) = match parse_condition(input) {
        Ok((input, filter)) => (input, filter),
        Err(x) => {
            error!("Error: {:?}", x);
            return Err(SyntaxError::ParseError(format!(
                "Could not parse condition: {:?}",
                x
            )));
        }
    };

    let query = Query {
        db: db.to_string(),
        table_name: table_name.to_string(),
        filter,
    };

    Ok(Command::Select(query))
}

fn parse_condition(input: &str) -> IResult<&str, Condition> {
    let (input, _) = multispace0(input)?;
    let (input, first_condition) = parse_complex_condition(input)?;
    let (input, conditions) = many0(tuple((
        preceded(multispace0, alt((tag("AND"), tag("OR")))),
        preceded(multispace0, parse_complex_condition),
    )))(input)?;

    let condition = conditions
        .into_iter()
        .fold(first_condition, |acc, (op, next)| {
            if op == "AND" {
                Condition::And(Box::new(acc), Box::new(next))
            } else {
                Condition::Or(Box::new(acc), Box::new(next))
            }
        });

    Ok((input, condition))
}

fn parse_complex_condition(input: &str) -> IResult<&str, Condition> {
    if input.starts_with('(') && input.ends_with(')') {
        //#FIXME A hack to remove the brackets, should use nom to do this
        let input = input
            .strip_prefix('(')
            .unwrap_or(input)
            .strip_suffix(')')
            .unwrap_or(input);
        return parse_condition(input);
    }

    parse_simple_condition(input)
    // alt((
    //     map(
    //         delimited(
    //             char('('),
    //             cut(parse_condition),
    //             char(')'),
    //         ),
    //         |condition| condition,
    //     ),
    //     parse_simple_condition,
    // ))(input)
    // Ok((" AND (name = 'John' OR age >= 30)", Condition::Equal("id".to_string(), "123".to_string())))
}

fn parse_simple_condition(input: &str) -> IResult<&str, Condition> {
    alt((
        map(
            tuple((
                take_while(|c: char| c.is_alphanumeric() || c == '_'),
                delimited(multispace0, tag("="), multispace0),
                parse_value,
            )),
            |(field, _, value)| Condition::Equal(field.to_string(), value),
        ),
        map(
            tuple((
                take_while(|c: char| c.is_alphanumeric() || c == '_'),
                delimited(multispace0, tag(">="), multispace0),
                parse_value,
            )),
            |(field, _, value)| Condition::GreaterThanOrEqual(field.to_string(), value),
        ),
    ))(input)
}

fn parse_value(input: &str) -> IResult<&str, String> {
    alt((
        delimited(
            char('\''),
            take_while(|c: char| c.is_alphanumeric() || c == '_'),
            char('\''),
        ),
        take_while1(|c: char| c.is_alphanumeric() || c == '_'),
    ))(input)
    .map(|(next_input, res)| (next_input, res.to_string()))
}

fn extract_select_table(input: &str) -> IResult<&str, &str> {
    delimited(space0, alpha1, tag(" "))(input)
}

#[cfg(test)]
mod tests {

    use core::panic;

    use super::*;

    #[test]
    fn test_parse_insert_command() {
        let db = "db";
        let message = r#"INSERT INTO user {"name":"John","age":30}"#;
        if let Command::Insert(inser_data) = parse_insert_command(db, message).unwrap() {
            assert_eq!(inser_data.table, ("user"));
            match inser_data.data {
                DataObject::Object(data) => {
                    assert_eq!(data[1].key, "name");
                    match &data[1].value {
                        DataObject::String(s) => {
                            assert_eq!(s.as_str(), "John")
                        }
                        _ => panic!("Expected string"),
                    };
                    assert_eq!(data[0].key, "age");
                    match &data[0].value {
                        DataObject::Number(n) => {
                            assert_eq!(n, &30.into())
                        }
                        _ => panic!("Expected number"),
                    };
                }
                _ => {
                    panic!("Expected object");
                }
            }
        } else {
            panic!("Expected Insert command");
        }
    }

    #[test]
    fn test_parse_delete_command() {
        let db = "db";
        let message = r#"DELETE FROM user { "id": "123", "name":"John","age":30}"#;
        if let Command::Delete(delete_data) = parse_delete_command(db, message).unwrap() {
            assert_eq!(delete_data.table, "user");
            match delete_data.data {
                DataObject::Object(data) => {
                    assert_eq!(data[1].key, "id");
                    match &data[1].value {
                        DataObject::String(s) => {
                            assert_eq!(s.as_str(), "123")
                        }
                        _ => panic!("Expected string"),
                    };
                    assert_eq!(data[0].key, "age");
                    match &data[0].value {
                        DataObject::Number(n) => {
                            assert_eq!(n, &30.into())
                        }
                        _ => panic!("Expected number"),
                    };
                    assert_eq!(data[2].key, "name");
                    match &data[2].value {
                        DataObject::String(s) => {
                            assert_eq!(s.as_str(), "John")
                        }
                        _ => panic!("Expected number"),
                    };
                }
                _ => {
                    panic!("Expected object");
                }
            }
        } else {
            panic!("Expected Delete command");
        }
    }

    #[test]
    fn test_parse_defile_command() {
        let message = r#"DEFINE user { "name": { "type": "String", "indexed": true, "optional": false }, "age": { "type": "Number", "indexed": false, "optional": true }}"#;
        match parse_define_command(message) {
            Ok(command) => match command {
                Command::Define(table, define) => {
                    assert_eq!(table, "user");
                    assert_eq!(define.len(), 2);
                    assert!(define.contains_key("name"));
                    assert!(define.contains_key("age"));
                    let name = define.get("name").unwrap();
                    assert_eq!(name.data_type, "String");
                    assert!(name.indexed);
                    assert!(!name.optional);

                    let age = define.get("age").unwrap();
                    assert_eq!(age.data_type, "Number");
                    assert!(!age.indexed);
                    assert!(age.optional);
                }
                _ => {
                    panic!("Expected Define command");
                }
            },
            Err(e) => {
                panic!("Expected Define command but got {:?}", e);
            }
        }
    }

    #[test]
    fn test_parse_select_command() {
        let message = r#"SELECT user WHERE id = '123' AND (name = 'John' OR age >= 30)"#;
        match parse_select("db", message) {
            Ok(command) => match command {
                Command::Select(query) => {
                    assert_eq!(query.table_name, "user");
                    match query.filter {
                        Condition::And(left, right) => {
                            match *left {
                                Condition::Equal(field, value) => {
                                    assert_eq!(field, "id");
                                    assert_eq!(value, "123");
                                }
                                _ => {
                                    panic!("Expected Equal operation");
                                }
                            }
                            match *right {
                                Condition::Or(left, right) => {
                                    match *left {
                                        Condition::Equal(field, value) => {
                                            assert_eq!(field, "name");
                                            assert_eq!(value, "John");
                                        }
                                        _ => {
                                            panic!("Expected Equal operation");
                                        }
                                    }
                                    match *right {
                                        Condition::GreaterThanOrEqual(field, value) => {
                                            assert_eq!(field, "age");
                                            assert_eq!(value, "30");
                                        }
                                        _ => {
                                            panic!("Expected GreaterThanOrEqual operation");
                                        }
                                    }
                                }
                                _ => {
                                    panic!("Expected Or operation");
                                }
                            }
                        }
                        _ => {
                            panic!("Expected And operation");
                        }
                    }
                }
                _ => {
                    panic!("Expected Select command");
                }
            },
            Err(e) => {
                panic!("Expected Select command but got {:?}", e);
            }
        }
    }

    #[test]
    fn test_parse_condition() {
        let message = r#"id = '123' AND (name = 'John' OR age >= 30)"#;
        match parse_condition(message) {
            Ok((x, condition)) => match condition {
                Condition::And(left, right) => {
                    match *left {
                        Condition::Equal(field, value) => {
                            assert_eq!(field, "id");
                            assert_eq!(value, "123");
                        }
                        _ => {
                            panic!("Expected Equal operation");
                        }
                    }
                    match *right {
                        Condition::Or(left, right) => {
                            match *left {
                                Condition::Equal(field, value) => {
                                    assert_eq!(field, "name");
                                    assert_eq!(value, "John");
                                }
                                _ => {
                                    panic!("Expected Equal operation");
                                }
                            }
                            match *right {
                                Condition::GreaterThanOrEqual(field, value) => {
                                    assert_eq!(field, "age");
                                    assert_eq!(value, "30");
                                }
                                _ => {
                                    panic!("Expected GreaterThanOrEqual operation");
                                }
                            }
                        }
                        _ => {
                            panic!("Expected And operation");
                        }
                    }
                }
                _ => {
                    panic!("Expected And operation");
                }
            },
            Err(e) => {
                panic!("Expected Select command but got {:?}", e);
            }
        }
    }

    #[test]
    fn test_parse_value_char() {
        let input = r#"'John'"#;
        let result = parse_value(input);
        match result {
            Ok((_, value)) => {
                assert_eq!(value, "John");
            }
            Err(e) => {
                panic!("Expected value but got {:?}", e);
            }
        }
    }

    #[test]
    fn test_parse_value_numbers() {
        let input = r#"30"#;
        let result = parse_value(input);
        match result {
            Ok((_, value)) => {
                assert_eq!(value, "30");
            }
            Err(e) => {
                panic!("Expected value but got {:?}", e);
            }
        }
    }
}
