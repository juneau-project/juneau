#!/usr/bin/env bash

psql -h postgres -p 5432 -d postgres -U postgres -f lshe_initialize.sql -f minhash.sql -f \
corpus_construct_sig.sql -f corpus_construct_hash.sql -f q_construct_sig.sql -f q_construct_hash.sql \
-f q_query_lshe.sql -f q_query_ks.sql