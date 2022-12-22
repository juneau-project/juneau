FROM zives/hab-postgres:15.1

RUN apt-get -y update

RUN apt install -y postgresql-server-dev-15 gcc g++

RUN mkdir /juneau_funcs

COPY ./join-size /juneau_funcs/join-size

COPY ./sketch /juneau_funcs/sketch

WORKDIR /juneau_funcs/join-size/c

RUN cc -fPIC -c -I /usr/include/postgresql/15/server/ join_score.cpp score.cpp

RUN cc -shared -o join_score.so join_score.o score.o

WORKDIR /juneau_funcs/sketch/c/ks

RUN cc -fPIC -c -I /usr/include/postgresql/15/server/ ks.cpp hist.cpp evaluate.cpp

RUN cc -shared -o ks.so ks.o hist.o evaluate.o

WORKDIR /juneau_funcs/sketch/c/lshe

RUN cc -fPIC -c -I /usr/include/postgresql/15/server/ -Ifnv/ fnv/hash_64a.c evaluate.cpp hash.cpp lshe.cpp probability.cpp sig.cpp

RUN cc -shared -o lshe.so hash_64a.o evaluate.o hash.o lshe.o probability.o sig.o

ENV POSTGRES_DB="postgres"

RUN psql -U postgres -f /juneau_funcs/join-size/sql/initialize_join_score.sql

RUN psql -U postgres /juneau_funcs/sketch/sql/initialize_sketch.sql