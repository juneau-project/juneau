# TEST evaluate.cpp
# rm ./evaluate
# rm *.o
# g++ -c evaluate.cpp
# g++ -o evaluate evaluate.o
# ./evaluate

# rm ./hist
# rm *.o
# g++ -c hist.cpp
# g++ -o hist hist.o
# ./hist

# SQL COMPILE
# rm *.o
# rm *.so
cc -c -I/Library/PostgreSQL/12/include/postgresql/server/ ks.cpp hist.cpp evaluate.cpp
cc -bundle -flat_namespace -undefined suppress -o ks.so ks.o hist.o evaluate.o


# rm ./test
# rm *.o
# g++ -c test.cpp
# g++ -o test test.o
# ./test

# rm ./test2
# rm *.o
# g++ -c test2.cpp
# g++ -o test2 test2.o
# ./test2