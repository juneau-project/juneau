## Incremental corpus sketches

- Initialize postgres by running `initialize_sketch.sql` and then the remaining `.sql` files

- Run the following commands only at the first time
```sql
-- run this line until all tables are hashed before running the next line
select sketch.corpus_sketch('schema', 128, #num_digits, #limit_num);

-- hash transpose_table
select sketch.corpus_transpose(128);

select sketch.corpus_partition(#partition);
```

- Thereafter, run the following commands
```sql
delete from sketch.partition_table;

-- run this line until all tables are hashed before running the next line
select sketch.corpus_hash('schema', 128, #num_digits, #limit_num);

select sketch.corpus_partition(#partition);
```

- REPLACE `'schema'` with the actual schema name
- REPLACE `#partition` with the actual number of partitions (typically 20)**
- REPLACE `#limit_num` with the number of tables you want to process at a single time
- REPLACE `#num_digits` with the number of digits you want to remove (if set as 0 then by default take the first 4 digits)