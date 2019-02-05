/*
 * Implementation of R&D assignment, benchmarking a set of queries against a 
 * hypertable containing series of CPU usage data
 * Author: Igor Kouznetsov
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <errno.h>
#include <unistd.h>
#include <libgen.h>
#include <pthread.h>
#include <time.h>
#include <math.h>
#include <float.h>
#include <vector>
#include <map>
#include <string>
#include <algorithm>

#include <libpq-fe.h> 

// max allowed number of workers; set as deemed reasonable
const int max_num_workers = 50;
int dbg = 0; // if 1, some debug info is printed

// result of input CSV line parsing
struct QueryParam
{
    std::string host;
    std::string start_time;
    std::string end_time;
};

// variables of this type will be passed to individual workers
// to be used to generate SQL queries
typedef std::vector<QueryParam> QueryParamArray;

// structure to pass to worker function
struct WorkerInfo
{
    WorkerInfo(): worker_slot(-1), query_param_array_p(NULL) {}
    pthread_t thread;
    int worker_slot; // if -1, slot is not in use
    QueryParamArray *query_param_array_p; // for faster reference in worker func
};

// final stats from individual worker
struct WorkerOutput 
{
    WorkerOutput(): total_queries(0), total_time(0), min_time(0), max_time(0) {}
    double total_queries;
    double total_time;
    double min_time;
    double max_time;
    std::vector<double> all_times;
};

// forward declarations
void error_out(const char *format, ...);
void print_usage(char *prog_name);
void parse_query_param_line(char *line, int line_no, QueryParam &param);
void *worker_func(void *arg);
void exit_gracefully(PGconn *conn);
void execute_query(PGconn *conn, const char *query);

// global data area 
std::vector<QueryParamArray> all_query_param_arrays;
std::vector<WorkerOutput> worker_output_array;

int main(int argc, char* argv[]) 
{
    errno = 0; // workaround for libpq errno problem, need to reset

    // first, parse the arguments
    
    int opt;
    FILE *in_file = stdin;
    int num_workers = 0;
    char prog_name[256];
    
    strcpy(prog_name, argv[0]);

    if(argc < 2) 
    {
        print_usage(prog_name);
    }    
    
    while((opt = getopt(argc, argv, ":hvn:f:")) != -1)  
    {  
        switch(opt)  
        {  
            case 'h':
                print_usage(prog_name);
                exit(EXIT_SUCCESS);
            case 'v':
                dbg = 1;
                break;
            case 'f':  
                in_file = fopen(optarg, "r");
                if(in_file == NULL) 
                {
                    error_out("cannot open input file %s (errno=%d)", optarg, errno);
                }
                break;
            case 'n':
                num_workers = strtol(optarg, NULL, 10);
                if(errno > 0 || num_workers <= 0 || num_workers > max_num_workers)
                {
                    print_usage(prog_name);
                    error_out("invalid value for argument -n: %s", optarg);
                }
                break;
            case ':':  
                print_usage(prog_name);
                error_out("option %s needs a value", argv[optind-1]); 
            case '?':  
                print_usage(prog_name);
                error_out("unknown option: %c", optopt); 
        }  
    }  
      
    if(optind < argc)
    {  
        print_usage(prog_name);
        error_out("unexpected argument: %s", argv[optind]);  
    }
    
    if(num_workers == 0) 
    {
        print_usage(prog_name);
        error_out("missing mandatory argument -n <num_workers>");
    }

    
    // now parse the input into internal representation 
    // ready to be fed to workers, initializing workers' input data,
    
    char line[1024];
    int line_no = 1; // for CSV error location reporting
    int real_num_workers = 0; // how many slots actually used
    std::vector<WorkerInfo> worker_info_array(num_workers);

    // initialize global data area
    all_query_param_arrays.resize(num_workers);
    worker_output_array.resize(num_workers);
    
    // skip the header line
    fgets(line, sizeof(line), in_file);
    
    while (fgets(line, sizeof(line), in_file))
    {
        line_no++;
        QueryParam query_param;
        parse_query_param_line(line, line_no, query_param);
        
        // determine the slot for this host by simple hash
        std::hash<std::string> str_hash;
        size_t slot = str_hash(query_param.host) % num_workers;
        
        if(dbg) 
        {
            fprintf(stderr, "debug: adding to slot %lu: %s, %s, %s\n",
                slot, 
                query_param.host.c_str(), 
                query_param.start_time.c_str(), 
                query_param.end_time.c_str()
            );
        }
        
        if(worker_info_array[slot].worker_slot == -1) { // new worker
            worker_info_array[slot].worker_slot = slot;
            real_num_workers++;
        }
        
        all_query_param_arrays[slot].push_back(query_param);
    }
    
    if(in_file != stdin)
        fclose(in_file);
    
    // if we have work to do, start the workers
    
    if(line_no == 1) // either no input or header only
    {
        fprintf(stderr, "info: no input CSV content, exiting\n");
        return EXIT_SUCCESS;
   }
    
    for (int i = 0; i < num_workers; i++) 
    {
        if(worker_info_array[i].worker_slot == -1)
            continue; // slot not in use
        worker_info_array[i].thread = pthread_t();
        worker_info_array[i].query_param_array_p = &all_query_param_arrays[i];
                
        int rc = 0;
        if ( (rc = pthread_create(
                &worker_info_array[i].thread, 
                NULL, 
                worker_func, 
                (void*)&worker_info_array[i]) 
            ) 
        )
        {
            error_out("failed to create thread num %d, error code=%d", i, rc);
        }
    }
    
    // wait for all workers to complete
    
    for (int i = 0; i < num_workers; i++) 
    {
        if(worker_info_array[i].worker_slot == -1)
            continue; // slot not in use
        pthread_join(worker_info_array[i].thread, NULL);
    }
    
    // calculate the final stats
    
    int total_queries = 0;
    double 
      total_time  = 0, 
      min_time    = DBL_MAX, 
      max_time    = 0, 
      avg_time    = 0, 
      median_time = 0;

    // combines all query times from all workers, to get the median
    std::vector<double> all_times; 
    
    for(int i = 0; i < num_workers; i++) 
    {
        if(worker_info_array[i].worker_slot == -1)
            continue; // slot not in use
        total_time += worker_output_array[i].total_time;
        total_queries += worker_output_array[i].total_queries;
        min_time = fmin(min_time, worker_output_array[i].min_time);
        max_time = fmax(max_time, worker_output_array[i].max_time);
        all_times.insert(
            all_times.end(), 
            worker_output_array[i].all_times.begin(), 
            worker_output_array[i].all_times.end()
        );
    }
    avg_time = total_time / total_queries;
    
    // get the median time
    std::sort(all_times.begin(), all_times.end());
    
    size_t half = all_times.size() / 2;
    if(all_times.size() % 2) 
        median_time = (all_times[half] + all_times[half +1]) / 2;
    else 
        median_time = all_times[half];
    
    fprintf(stdout, 
        "Benchmark statistics (all times are in seconds with ns granularity):\n"
        "Total # of queries: %15d\n"
        "Real  # of workers: %15d\n"
        "Query execution times:\n"
        "Total:              %15.9lf\n"
        "Minimum:            %15.9lf\n"
        "Maximum:            %15.9lf\n"
        "Average:            %15.9lf\n"
        "Median:             %15.9lf\n",
        total_queries,
        real_num_workers,
        total_time,
        min_time,
        max_time,
        avg_time,
        median_time
    );
    
    return EXIT_SUCCESS;
}

void error_out(const char * format, ...)
{
    fprintf(stderr, "error: ");
    va_list args;
    va_start (args, format);
    vfprintf(stderr, format, args);
    va_end (args);
    fputc('\n', stderr);
    exit(EXIT_FAILURE);
}

void print_usage(char *prog_name)
{
    fprintf(stderr, 
            "Benchmark SQL queries against hypertable with sample data\n"
            "Usage: %s [-h] -n <num_workers> [-f <in_file>] [-v]\n"
            "Arguments:\n"
            "  -h -- print this screen\n"
            "  -n -- the number of worker threads between 1 and %d\n"
            "  -f -- the input CSV file name containing the queries' parameters.\n"
            "        If omitted, standard input is assumed\n"
            "  -v -- verbose; print some debug output\n",
            basename(prog_name), max_num_workers
    );
}

void parse_query_param_line(char *line, int line_no, QueryParam &query_param)
{
    const char* tok;
    int field_no = 0; // we'll expect 3 fields
    
    // we don't care to duplicate the line before strtok() call, 
    // it'll be no longer needed
    
    for (tok = strtok(line, ",");
            tok && *tok;
            tok = strtok(NULL, ",\n"), field_no++)
    {
        switch(field_no) 
        {
            case 0: query_param.host       = tok; break;
            case 1: query_param.start_time = tok; break;
            case 2: query_param.end_time   = tok; break;
            default: break; 
        }
    }
    // we'll just validate the number of fields here; 
    // the rest is deferred to postgres execution
    if(field_no != 3) 
        error_out("wrong number of fields: %d in input line %d", field_no, line_no);
}

void *worker_func(void *arg)
{
    WorkerInfo *worker_info = (WorkerInfo*)arg;
    int worker_slot = worker_info->worker_slot;

    if(dbg)
        fprintf(stderr, "debug: started worker %d\n", worker_slot);

    // this points to element in global area; use this for quick access
    QueryParamArray *query_param_array_p = worker_info->query_param_array_p; 
    
    int total_queries = 0;
    double 
        min_time   = DBL_MAX, 
        max_time   = 0, 
        total_time = 0;
        std::vector<double> all_times;

    // establish postgres connection for this worker
    // modify this per your setup
    // to avoid exposing password in the code, use the ~/.pgpass file
    // I hardcoded the info per my setup, yours is probably different

    int conn_info_line_no = __LINE__ + 1;
    const char *conn_info = "dbname=homework user=postgres password=postgres";
    PGconn     *conn;

    conn = PQconnectdb(conn_info);

    if (PQstatus(conn) != CONNECTION_OK) 
    {
        fprintf(stderr, 
            "error: connection to database failed, error message: %s\n",
            PQerrorMessage(conn)
        );
        fprintf(
            stderr, "Hint: check connection string at %s:%d\n", 
            __FILE__, 
            conn_info_line_no
        );
        exit_gracefully(conn);
    }

    // traverse through all query parameters
    for(int i = 0; i < query_param_array_p->size(); i++) 
    {
        // generate the query from this workers input parameters
        char query[2048];
        snprintf(query, sizeof(query), 
            "SELECT time_bucket('1 minute', ts), MIN(usage), MAX(usage) "
            "FROM cpu_usage "
            "WHERE host='%s' AND ts BETWEEN '%s' AND '%s' "
            "GROUP BY 1",
            (*query_param_array_p)[i].host.c_str(),
            (*query_param_array_p)[i].start_time.c_str(),
            (*query_param_array_p)[i].end_time.c_str()
        );
        if(dbg)
            fprintf(stderr, "debug: from wkr %d: '%s'\n", worker_slot, query);

        // execute the query, measuring execution time
        struct timespec query_start, query_end;
        clock_gettime(CLOCK_MONOTONIC, &query_start);
        execute_query(conn, query);
        clock_gettime(CLOCK_MONOTONIC, &query_end);
        
        // query taken by the query, in seconds
        double query_time = (
            pow(10, 9) * query_end.tv_sec + query_end.tv_nsec - 
            pow(10, 9) * query_start.tv_sec - query_start.tv_nsec
        ) / pow(10, 9);
        
        total_queries++;
        total_time += query_time;
        min_time = fmin(min_time, query_time);
        max_time = fmax(max_time, query_time);
        
        // for median calculation on global level
        all_times.push_back(query_time);
    }
    
    PQfinish(conn);

    // populate the global output area -- no synchronization needed
    worker_output_array[worker_slot].total_queries = total_queries;
    worker_output_array[worker_slot].total_time    = total_time;
    worker_output_array[worker_slot].min_time      = min_time;
    worker_output_array[worker_slot].max_time      = max_time;
    worker_output_array[worker_slot].all_times     = all_times;
}

void exit_gracefully(PGconn *conn)
{
    PQfinish(conn);
    exit(EXIT_FAILURE);
}

void execute_query(PGconn *conn, const char *query)
{
    PGresult   *res;

    res = PQexec(conn, query);
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, 
            "error: query failed.\nError message: %s\nQuery: \"%s\"\n", 
            PQerrorMessage(conn), query
        );
        PQclear(res);
        exit_gracefully(conn);
    }
    
    if(dbg) {
        fprintf(stderr, "rows: %d, 1st row: bucket=%s, min=%s, max=%s\n",
                PQntuples(res),
                PQgetvalue(res, 0, 0),
                PQgetvalue(res, 0, 1),
                PQgetvalue(res, 0, 2)
        );
    }
    
    PQclear(res);
}