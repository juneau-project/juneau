# Database Schema Usage

## PostgreSQL

Juneau expects to create its own schemas in PostgreSQL, for different purposes.

The database is set using the `sql_name` config variable, at the `sql_host` location.

There are three separate schemas.

### `sql_graph` 

(default `graph_model`)

This schema tracks the line-to-line dependencies.

* `dependen(view_id, view_cmd)`
* `line2cid(view_id,view_cmd)`
* `lastliid(view_id,view_cmd)`

### nb_provenance

* `code_dict`

### rowstore

Stores the actual variable content, ie the tables, using a canonical naming scheme

## Neo4J

Stores the cells themselves, including cell to cell relationships
