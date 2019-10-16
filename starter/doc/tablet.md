# List Tables

List all tables that are present on the tablet.

**URL** : `/api/tables`

**Method** : `GET`

**Input Data** : None

## Responses

**Condition** : No tables are defined.

**Code** : `200 OK`

**Content** : 
```json
{
    "tables": []
}
```

### OR

**Condition** : One or more tables are defined.

**Code** : `200 OK`

**Content** : 
```json
{
    "tables": [
        "table1",
        "table2"
    ]
}
```

# Create Table

Create a new table.

**URL** : `/api/tables`

**Method** : `POST`

**Input Data** : 
```json
{
    "name": "table1",
    "column_families": [
        {
            "column_family_key": "key1",
            "columns": ["column_key1", "column_key2"]
        }, 
        {
            "column_family_key": "key2",
            "columns": ["column_key3", "column_key3"]           
        }
    ]
    
}
```

## Responses

**Condition** : Success - Table Not Already Present.

**Code** : `200 OK`

**Content** : NIL

### OR

**Condition** : Failure - Table exists.

**Code** : `409 Conflict`

**Content** : NIL

### OR

**Condition** : Failure - Cannot parse JSON.

**Code** : `400 Bad Request`

**Content** : NIL

# Destroy Table

Destroy a specific table

**URL** : `/api/tables/:pk`

**Method** : `DELETE`

**Input Data** : NIL

## Responses

**Condition** : Success - Table Not in Use

**Code** : `200 OK`

**Content** : NIL

### OR

**Condition** : Failure - Table Does Not Exist

**Code** : `404 Not Found`

**Content** : NIL

### OR

**Condition** : Failure - Table in Use **(Not Required in Checkpoint 1)**

**Code** : `409 Conflict`

**Content** : NIL

# Get Table Info

Get information about a specific table.

**URL** : `/api/tables/:pk`

**URL Parameters** : `pk=[string]` where `pk` is the table name.

**Method** : `GET`

**Input Data** : NIL

## Responses

**Condition** : Table does not exist.

**Code** : `404 Not Found`

**Content** : NIL

### OR

**Condition** : Table exists.

**Code** : `200 OK`

**Content** : 
```json
{
    "name": "table1",
    "column_families": [
        {
            "column_family_key": "key1",
            "columns": ["column_key1", "column_key2"]
        }, 
        {
            "column_family_key": "key2",
            "columns": ["column_key3", "column_key3"]           
        }
    ]
    
}
```

# Insert a cell

Insert a value into the database.

**URL** : `/api/table/:pk/cell`

**URL Parameters** : `pk=[string]` where `pk` is the table name.

**Method** : `POST`

**Input Data** : 
```json
{
    "column_family": "hello",
    "column": "world",
    "row": "sample",
    "data": [
        {
            "value": "i am a very long string...",
            "time" : 1570212112.504831,
        }
    ]
}
```

## Responses

**Condition** : Table does not exist.

**Code** : `404 Not Found`

**Content** : NIL

### OR

**Condition** : Column family or column does not exist.

**Code** : `400 Bad Request`

**Content** : NIL

### OR

**Condition** : Insert success.

**Code** : `200 OK`

**Content** : NIL


# Retrieve a cell

Retrieve a cell from the database specified row, column family and column key

**URL** : `/api/table/:pk/cell`

**URL Parameters** : `pk=[string]` where `pk` is the table name.

**Method** : `GET`

**Input Data - Single Row** : 
```json
{
    "column_family": "hello",
    "column": "world",
    "row": "sample"
}
```

## Responses

**Condition** : Table does not exist.

**Code** : `404 Not Found`

**Content** : NIL

### OR

**Condition** : Column family or column does not exist.

**Code** : `400 Bad Request`

**Content** : NIL

### OR

**Condition** : Retrieve success.

**Code** : `200 OK`

**Content** :

**Content - Single Data cell for specified row, column family and column key ** : 
```json
{
    "row": "row_a",
    "data": [
        {
            "value": "i am a data",
            "time": 1570212112.504831,
        },
        {
            "value": "i am another data",
            "time": 1570212112.504831,
        }
    ]
}
```

# Retrieve cells

Retrieve cells from the database for specified row range, a column family and a column key 

**URL** : `/api/table/:pk/cells`

**URL Parameters** : `pk=[string]` where `pk` is the table name.

**Method** : `GET`

**Input Data - Row Range** : 
```json
{
    "column_family": "hello",
    "column": "world",
    "row_from": "sample_a",
    "row_to": "sample_x"
}
```
## Responses

**Condition** : Table does not exist.

**Code** : `404 Not Found`

**Content** : NIL

### OR

**Condition** : Retrieve success.

**Code** : `200 OK`

**Content** :

**Content - Range of cells for specified row range, a column family and a column key ** : 
```json
    {"rows": [
                {
                    "row": "row1",
                    "data": [
                        {
                            "value": "i am a data",
                            "time": 1570212112.504831,
                        },
                        {
                            "value": "i am another data",
                            "time": 1570212112.504831,
                        }
                    ]  
                },
                {
                    "row": "row2",
                    "data": [
                        {
                            "value": "hello world",
                            "time": 123
                        }
                    ]
                }
            ]
   }
```


# Retrieve a row of table

Retrieve value(s) from the database.

**URL** : `/api/table/:pk/row`

**URL Parameters** : `pk=[string]` where `pk` is the table name.

**Method** : `GET`

**Input Data - Single Row** : 
```json
{
    "row": "sample"
}
```

## Responses

**Condition** : Table does not exist.

**Code** : `404 Not Found`

**Content** : NIL

### OR

**Condition** : Retrieve success.

**Code** : `200 OK`

**Content** :

**Content - Specified row of the table along with all its column families: 
```json
{
    "row": "row_a",
    "column_families": [
        {"hello": 
            {"columns": [
                {"world":
                    {"data": [
                        {
                            "value": "i am a data",
                            "time": 1570212112.504831,
                        }
                    ]
                    }
                }
            ]
            }
        }        
    ]
}
```

# Set MemTable Max Entries 

Change MemTable max.

**URL** : `/api/memtable`

**Method** : `POST`

**Input Data** : 
```json
{
    "memtable_max": 30
}
```

## Responses

**Condition** : Bad memtable max

**Code** : `400 Bad Request`

**Content** : NIL

### OR

**Condition** : Changed Memtable max

**Code** : `200 OK`

**Content** : NIL