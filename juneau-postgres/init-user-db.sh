#!/bin/bash
set -e

psql -U postgres -f /juneau_funcs/join-size/sql/initialize_join_score.sql
psql -U postgres /juneau_funcs/sketch/sql/initialize_sketch.sql
