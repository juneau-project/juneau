# FOR COMPILING lshe.cpp only (without the fnv folder)
# cc -c -I/Library/PostgreSQL/12/include/postgresql/server/ lshe.cpp sig.cpp
# cc -bundle -flat_namespace -undefined suppress -o lshe.so lshe.o sig.o

# COMPILATION FOR LOCAL TESTING
# rm *.o
# gcc -c -o fnv.o fnv/hash_64a.c
# g++ -c -o sig.o sig.cpp
# g++ -o sig fnv.o sig.o

# HASH: LOCAL TESTING
# rm hash
# gcc -c -o fnv.o fnv/hash_64a.c
# g++ -c -o sig.o sig.cpp
# g++ -c -o hash_local.o hash_local.cpp
# g++ -o hash fnv.o sig.o hash_local.o

# SQL COMPILE
# rm *.o
# rm *.so
cc -c -I/Library/PostgreSQL/12/include/postgresql/server/ -Ifnv/ fnv/hash_64a.c evaluate.cpp hash.cpp lshe.cpp probability.cpp sig.cpp
cc -bundle -flat_namespace -undefined suppress -o lshe.so hash_64a.o evaluate.o hash.o lshe.o probability.o sig.o

# rm hash
# rm *.o
# rm *.so
# g++ -c -o prob.o probability.cpp
# g++ -o prob prob.o

# rm *.o
# rm *.so
# rm evaluate
# g++ -o evaluate evaluate.cpp
# ./evaluate


# rm *.o
# rm *.so
# rm hash
# g++ -o hash hash.cpp
# ./hash