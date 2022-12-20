# rm ./test
# rm *.o
# g++ -c test.cpp
# g++ -o test test.o
# ./test

rm *.o
rm *.so
cc -c -I/Library/PostgreSQL/12/include/postgresql/server/ join_score.cpp score.cpp
cc -bundle -flat_namespace -undefined suppress -o join_score.so join_score.o score.o