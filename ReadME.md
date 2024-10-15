xdr:DEFINE user { "name": { "type": "String", "indexed": true, "optional": false }, "age": { "type": "Number", "indexed": false, "optional": true}}
xdr:INSERT INTO user {"name":"John","last_name":"SMITH","age":30}
xdr:SELECT user WHERE name = 'John' and age >= 30
xdr:INSERT INTO user {"name":"Jasun","last_name":"SMITH","age":30}
UPDATE user {"name":"John","age":30} WHERE name = 'John'
