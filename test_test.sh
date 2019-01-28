#!/bin/bash
#
#  A simple functionality test for pq_bench_test.
#  Note that it is prone to changes of the program's output, 
#  so if you do change the output, make sure you change this 
#  script accordingly.
#  To see if this script is working properly, you can 
#  modify it to e.g. inverse some assertion, comment out the
#  output filtering/suppression to see the actual output, etc.  
#
# Author: Igor Kouznetsov

PROG=$(basename $0)

function main()
{
    test_help_screen
    test_invalid_args
    check_db_connection
    test_empty_input
    test_invalid_input
    test_invalid_fields_number
    test_valid_input
}

# simple assertion; you can pass a command to execute,
# or a a test condition, it will be evaluated, and if
# return code is non_zero, the test prints the evaluated 
# expression with the location of the assertion, and exits 
function assert
{
    cmd=$1
    eval $cmd
    if [ $? -ne 0 ]; then
        printf "\n$PROG:${BASH_LINENO[0]}: assertion failed: $cmd\n" 1>&2
        exit 1
    fi
}

function test_help_screen
{
    printf "check if help screen is working... "
    ./pq_bench_test -h 2>&1 | grep "Arguments:" > /dev/null
    assert "[ $? == 0 ]"
    echo OK
}

function test_invalid_args
{
    printf "check if invalid program arguments are detected... "
    ./pq_bench_test -n 0 2>&1 | grep "invalid value for argument -n" > /dev/null
    assert "[ $? == 0 ]"
    ./pq_bench_test -n blah 2>&1 | grep "invalid value for argument -n" > /dev/null
    assert "[ $? == 0 ]"
     ./pq_bench_test -n 1 -f i_dont_exist 2>&1 | grep "cannot open input file" > /dev/null
    assert "[ $? == 0 ]"
    ./pq_bench_test -n 2>&1 | grep "needs a value" > /dev/null
    assert "[ $? == 0 ]"
    ./pq_bench_test -n 1 -z 2>&1 | grep "unknown option" > /dev/null
    assert "[ $? == 0 ]"
    ./pq_bench_test -n 1 blah 2>&1 | grep "unexpected argument" > /dev/null
    assert "[ $? == 0 ]"
    ./pq_bench_test -v 2>&1 | grep "missing mandatory argument" > /dev/null
    assert "[ $? == 0 ]"
    echo OK
}

function check_db_connection
{
    # we do that by providing input that passes CSV parsing
    # so the DB connection is attempted 
    printf "check the db connection... "
    printf "\n1,2,3" | ./pq_bench_test -n 1 2>&1 | grep "connection to database failed" >/dev/null
    assert "[ $? != 0 ]"
    echo OK
}

function test_empty_input
{
    printf "check if exiting gracefully on empty CSV input... "
    printf "" | ./pq_bench_test -n 1 2>&1 | grep "no input CSV content" >/dev/null
    assert "[ $? == 0 ]"
    echo OK
}

function test_invalid_input
{
    printf "check if syntactically invalid CSV input is detected... "
    printf "\n1,2,3" | ./pq_bench_test -n 1 2>&1 | grep "invalid input syntax" >/dev/null
    assert "[ $? == 0 ]"
    echo OK
}

function test_invalid_fields_number
{
    printf "check if wrong number of fields in CSV input is detected... "
    printf "\n1" | ./pq_bench_test -n 1 2>&1 | grep "wrong number of fields" >/dev/null
    assert "[ $? == 0 ]"
    printf "\n1,2,3,4" | ./pq_bench_test -n 1 2>&1 | grep "wrong number of fields" >/dev/null
    assert "[ $? == 0 ]"
    echo OK
}

function test_valid_input
{
    printf "check if valid CSV processed successfully... "
    cat << EOF | ./pq_bench_test -n 1 2>&1 | egrep "Total # of queries: *2$" >/dev/null
hostname,start_time,end_time
host_000008,2017-01-01 08:59:22,2017-01-01 09:59:22
host_000001,2017-01-02 13:02:02,2017-01-02 14:02:02
EOF
    assert "[ $? == 0 ]"
    echo OK
}

main "$@"