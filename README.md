## pq_bench_test

This is the implementation of R&D assignment, benchmarking a set of queries against a hypertable containing series of CPU usage data.

Before building, see the note regarding authentication while establishing Postgres connection in `pq_bench_test.cpp`, function `worker_func()`.

To build, run `make -f build.mk` in the directory containing `pq_bench_test.cpp` and `build.mk`.

This will build the the standalone utility `pq_bench_test`, which does all the job.

It is assumed that you have the test environment setup as per the assignment description.

Run `./pq_bench_test -h` to see the usage, which is self-explanatory.

Any questions can be sent to Igor Kouznetsov at [mailto:kindslon@gmail.com]
