# Bigtable Test API 

## Tablet Server APIs

### Table

Get information about tables.

* [List Tables](tablet.md) : `GET /api/tables/`
* [Create Table](tablet.md) : `POST /api/tables/`
* [Delete Table](tablet.md) : `DELETE /api/tables/`
* [Get Table Info](tablet.md) : `GET /api/tables/:pk/`

### Operations

Ordinary Bigtable control functions.

* [Insert a cell](tablet.md) : `POST /api/table/:pk/cell`
* [Retrieve a cell](tablet.md) : `GET /api/table/:pk/cell`
* [Retrieve cells](tablet.md) : `GET /api/table/:pk/cells`

### Internal State

APIs that support the grading script.
* [Set MemTable Max Entries](tablet.md) : `POST /api/memtable/:pk/`

## Master Server APIs

### Table

Table-level requests

* [List Tables](master.md) : `GET /api/tables/`
* [Create Table](master.md) : `POST /api/tables/`
* [Delete Table](master.md) : `DELETE /api/tables/`
* [Get Table Info](master.md) : `GET /api/tables/:pk/`

### Locks

Allow clients to lock for use.

* [Open](tablet.md) : `POST /api/lock/:pk/`
* [Close](tablet.md) : `DELETE /api/lock/:pk/`

