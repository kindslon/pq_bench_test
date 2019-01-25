/*
 * 
 * pg_config --includedir
 * /usr/lib/x86_64-linux-gnu/

 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <libgen.h>
#include <pthread.h>
#include <time.h>
#include <math.h>
#include <vector>
#include <map>
#include <string>
#include <algorithm>

#include <libpq-fe.h>

// max allowed number of workers; set as deemed reasonable
const int max_num_workers = 50;

// host => worker assignment
typedef std::map<std::string, int> HostWorkerMap;

struct QueryParam
{
    std::string host;
    std::string start_time;
    std::string end_time;
};

// such arrays will be passed to individual workers
// to be used to generate SQL queries

typedef std::vector<QueryParam> QueryParamArray;

// such array will be filled by input CSV parsing; indexed by worker number

typedef std::vector<QueryParamArray> AllQueryParamArrays;

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

// each worker will write its stats to according element in this array
// (the index is the worker number)
typedef std::vector<WorkerOutput> WorkerOutputArray;

struct WorkerData 
{
    int worker_no;
    QueryParamArray *quare_param_array;
};

void print_usage(char *prog_name);
void parse_query_param_line(char *line, int line_no, QueryParam &param);
void *worker_func(void *arg);

// global data area
AllQueryParamArrays all_query_param_arrays;
WorkerOutputArray worker_output_array;

int main(int argc, char* argv[]) 
{
    // first, parse the arguments
    
    int opt;
    FILE *in_file = stdin;
    int num_workers = 0;
    char prog_name[256];
    
    strcpy(prog_name, argv[0]);

    if(argc < 2) {
        print_usage(prog_name);
    }    
    
    while((opt = getopt(argc, argv, ":hn:f:")) != -1)  
    {  
        switch(opt)  
        {  
            case 'h':
                print_usage(prog_name);
                exit(EXIT_SUCCESS);
            case 'f':  
                in_file = fopen(optarg, "r");
                if(in_file == NULL) {
                    fprintf(stderr, "Cannot open input file %s (errno=%d)\n", optarg, errno);
                    exit(EXIT_FAILURE);
                }
                break;
            case 'n':
                num_workers = strtol(optarg, NULL, 10);
                if(errno > 0 || num_workers <= 0 || num_workers > max_num_workers) {
                    print_usage(prog_name);
                    fprintf(stderr, "invalid value for argument -n: %s\n", optarg);
                    exit(EXIT_FAILURE);
                }
                break;
            case ':':  
                print_usage(prog_name);
                fprintf(stderr, "option %s needs a value\n", argv[optind-1]); 
                exit(EXIT_FAILURE);  
            case '?':  
                print_usage(prog_name);
                fprintf(stderr, "unknown option: %c\n", optopt); 
                exit(EXIT_FAILURE);
        }  
    }  
      
    if(optind < argc){  
        print_usage(prog_name);
        fprintf(stderr, "unexpected argument: %s\n", argv[optind]);  
    }
    
    if(num_workers == 0) {
        print_usage(prog_name);
        fprintf(stderr, "missing mandatory argument -n <num_workers>\n");
        exit(EXIT_FAILURE);
    }

    
    // now parse the input into internal representation 
    // ready to be fed to workers
    
    HostWorkerMap host_worker_map;
    
    char line[1024];
    int line_no = 1; // for CSV error location reporting
    int next_worker_no = 0; // next available worker slot index
    
    // skip the header line
    fgets(line, sizeof(line), in_file);
    line_no++;
    
    while (fgets(line, sizeof(line), in_file))
    {
        QueryParam query_param;
        parse_query_param_line(line, line_no, query_param);
        HostWorkerMap:: const_iterator iter = 
            host_worker_map.find(query_param.host);
        
        // if this host is already assigned to a worker, add this element to
        // this worker's query parameters, otherwise determine slot for new host
        int slot;
        if(iter != host_worker_map.end())
            slot = iter->second;
        else {
            // add host to map and assign it to next available worker
            slot = (next_worker_no < num_workers) ?
                next_worker_no : 0;
            // advance the pointer to next slot or reset if reached max
            if(next_worker_no+1 < num_workers)
                next_worker_no++;
            else
                next_worker_no = 0;

            host_worker_map.insert({query_param.host, slot});
        }
        fprintf(stderr, "adding to slot %d: %s, %s, %s\n",
                slot, query_param.host.c_str(), 
                query_param.start_time.c_str(), query_param.end_time.c_str());
        
        // if such slot doesn't exist, create new empty one
        if(slot+1 > all_query_param_arrays.size()){
            //QueryParamArray query_param_array;
            all_query_param_arrays.push_back(QueryParamArray());
            //all_query_param_arrays.push_back(query_param_array);
            //WorkerOutput 
            worker_output_array.push_back(WorkerOutput());
        }
        all_query_param_arrays[slot].push_back(query_param);
        
        line_no++;
    }
    
    if(in_file != stdin)
        fclose(in_file);

    // start the workers
    
    num_workers = all_query_param_arrays.size();
    
    struct ThreadElem
    {
        pthread_t thread;
        int worker_no;
    };
    std::vector<ThreadElem> threads_array;

    for (int i = 0; i < num_workers; i++) {
        //pthread_t thr;
        threads_array.push_back({pthread_t(), i});
        int rc = 0;
        if ( (rc = pthread_create(&threads_array[i].thread, NULL, worker_func, (void*)&threads_array[i].worker_no)) ) {
            fprintf(stderr, "failed to create thread num %d, rc: %d\n", i, rc);
            return EXIT_FAILURE;
        }
    }
        
    /* wait for all workers to complete */
    for (int i = 0; i < num_workers; i++) {
        pthread_join(threads_array[i].thread, NULL);
    }
    
    // calculate the final stats
    
    int total_queries = 0;
    double 
      total_time  = 0, 
      min_time    = 0, 
      max_time    = 0, 
      avg_time    = 0, 
      median_time = 0;

    // combines all query times from all workers, to get the median
    std::vector<double> all_times; 
    
    for(int i = 0; i < num_workers; i++) {
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
        "Benchmark statistics (all times are in seconds):\n"
        "Total # of queries:           %10d\n"
        "Total queries execution time: %10.3lf\n",
        total_queries,
        total_time
    );
    
    return EXIT_SUCCESS;
}

void print_usage(char *prog_name)
{
    fprintf(stderr, 
            "Benchmark SQL queries against hypertable with sample data\n"
            "Usage: %s [-h] -n <num_workers> [-f <in_file>]\n"
            "Arguments:\n"
            "  -h -- print this screen\n"
            "  -n -- the number of worker threads between 1 and %d\n"
            "  -f -- the input CSV file name containing the queries' parameters.\n"
            "        If omitted, standard input is assumed\n",
            basename(prog_name), max_num_workers
            );
}

void parse_query_param_line(char *line, int line_no, QueryParam &query_param)
{
    const char* tok;
    int field_no = 1; // we'll expect 3 fields
    
    // we don't care to duplicate the line before strtok() call, 
    // it'll be no longer needed
    
    for (tok = strtok(line, ",");
            tok && *tok;
            tok = strtok(NULL, ",\n"), field_no++)
    {
        switch(field_no) 
        {
            case 1: query_param.host       = tok; break;
            case 2: query_param.start_time = tok; break;
            case 3: query_param.end_time   = tok; break;
            default: break; 
        }
    }
    // we'll just validate the number of fields here; 
    // the rest is deferred to postgres execution
    if(field_no-1 != 3) {
        fprintf(
            stderr, "wrong number of fields %d in input line %d\n", 
            field_no, line_no
        );
        exit(EXIT_FAILURE);
    }
}

void *worker_func(void *arg)
{
    int worker_no = *(int*)arg;
 printf("in %d\n", worker_no);   
    // traverse through all query parameters
    for(int i = 0; i < all_query_param_arrays[worker_no].size(); i++) {
        // generate the query from this workers input parameters
        char query[2048];
        snprintf(query, sizeof(query), 
            "SELECT time_bucket('1 minute', ts), MIN(usage), MAX(usage) "
            "FROM cpu_usage "
            "WHERE host='%s' AND ts BETWEEN '%s' AND '%s' "
            "GROUP BY 1",
            all_query_param_arrays[worker_no][i].host.c_str(),
            all_query_param_arrays[worker_no][i].start_time.c_str(),
            all_query_param_arrays[worker_no][i].end_time.c_str()
        );
        fprintf(stderr, "from wkr %d: '%s'\n", worker_no, query);
        
        double min_time = 0, max_time = 0, total_time = 0;
        struct timespec query_start, query_end;
        clock_gettime(CLOCK_MONOTONIC, &query_start);
        
        // execute the query
        
        clock_gettime(CLOCK_MONOTONIC, &query_end);
        
        // query taken by the query, in seconds
        double query_time = (
            pow((double)query_end.tv_sec, 9) + query_end.tv_nsec -
            pow((double)query_start.tv_sec, 9) + query_start.tv_nsec
        ) / pow(10, 9);
        
        total_time += query_time;
        min_time = fmin(min_time, query_time);
        max_time = fmax(max_time, query_time);
        
        // populate the global output area -- no synchronization needed
        
        worker_output_array[worker_no].total_queries++;
        worker_output_array[worker_no].total_time += query_time;
        worker_output_array[worker_no].min_time = min_time;
        worker_output_array[worker_no].max_time = max_time;
        // for median calculation on global level
        worker_output_array[worker_no].all_times.push_back(query_time);
    }
}
