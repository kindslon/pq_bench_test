# Makefile for pq_bench_test utility
# Author: Igor Kouznetsov

CXX = g++
CXXFLAGS = -g -m64 -I`pg_config --includedir`
LDFLAGS = -lm -pthread -L`pg_config --libdir` -lpq

all: pq_bench_test

pq_bench_test: pq_bench_test.cpp
	${CXX} ${CXXFLAGS} -o $@ $@.cpp $(LDFLAGS)

clean:
	rm -f pq_bench_test.o pq_bench_test

