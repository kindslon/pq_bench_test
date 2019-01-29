## pq_bench_test

This is the implementation of R&D assignment, benchmarking a set of queries against a hypertable containing series of CPU usage data.

Before building, see the note regarding authentication while establishing Postgres connection in `pq_bench_test.cpp`, function `worker_func()`.

To build, run `make -f build.mk` in the directory containing `pq_bench_test.cpp` and `build.mk`.

This will build the the standalone utility `pq_bench_test`, which does all the job.

To remove all built files, run `make -f build.mk clean`.

It is assumed that you have the test environment setup as per the assignment description.

Run `./pq_bench_test -h` to see the usage, which is self-explanatory.

Example of execution:

```
./pq_bench_test -n 5 -f data_path/query_params.csv
```

or, to see some debug output:

```
./pq_bench_test -n 5 -f data_path/query_params.csv -v
```

```
Note:
-----

The package also contains a simple functionality test of the utility: 
`test_test.sh`, which does a bunch of assertions.

Since this script examines the output of the utility under different conditions 
it is called, it is prone to changes of the program's output, 
so if you do change the output, make sure you change this 
script accordingly.

It currently doesn't take any command line arguments.

To see if this script is working properly, you can 
modify it to e.g. inverse some assertion, comment out the
output filtering/suppression to see the actual output, etc.  
```

Any questions/suggestions can be sent to Igor Kouznetsov at [mailto:kindslon@gmail.com]
