# List Tables

List all tables that are present on the Bigtable system.

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

Create a new table. Master should forward the request to a tablet, ensure that it is complete, and provide the client with the tablet config details

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

**Content** : 
```json
{
    "hostname": "8.8.8.8",
    "port": "123"
}
```

### OR

**Condition** : Failure - Table exists.

**Code** : `409 Conflict`

**Content** : NIL

### OR

**Condition** : Failure - Cannot parse JSON.

**Code** : `400 Bad Request`

**Content** : NIL

# Destroy Table

Destroy a specific table. Master should forward this request to all tablet servers holding regions of this table.

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

**Condition** : Failure - Table in Use 

**Code** : `409 Conflict`

**Content** : NIL

# Get Table Info

Get information about a specific table. Return information about tablets holding ranges of the table.

**URL** : `/api/tables/:pk`

**URL Parameters** : `pk=[string]` where `pk` is the table id.

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
    "tablets": [
        {
            "hostname": "8.8.8.8",
            "port": "123",
            "row_from": "a",
            "row_to": "z"
        }
    ]
}
```

# Open

Allow a client to begin using an existing table. 

**URL** : `/api/lock/:pk`

**URL Parameters** : `pk=[string]` where `pk` is the table id.

**Method** : `POST`

**Input Data** : 
```json
{
    "client_id": "myid"
}
```

## Responses

**Condition** : Table does not exist.

**Code** : `404 Not Found`

**Content** : NIL

### OR

**Condition** : Client already opened the table.

**Code** : `400 Bad Request`

**Content** : NIL

### OR

**Condition** : Open success.

**Code** : `200 OK`

**Content** : NIL

# Close

Client relinquishes a table. 

**URL** : `/api/lock/:pk`

**URL Parameters** : `pk=[string]` where `pk` is the table id.

**Method** : `DELETE`

**Input Data** : 
```json
{
    "client_id": "myid"
}
```

## Responses

**Condition** : Table does not exist.

**Code** : `404 Not Found`

**Content** : NIL

### OR

**Condition** : Client did not open the table.

**Code** : `400 Bad Request`

**Content** : NIL

### OR

**Condition** : Close success.

**Code** : `200 OK`

**Content** : NIL
