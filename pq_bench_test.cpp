/*
 select time_bucket('1 minute', ts), sum(usage) 
 from cpu_usage 
 where ts between '2017-01-01 00:00:00' and '2017-01-01 00:59:00' 
 group by 1 order by 1 limit 200;
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
#include <vector>
#include <map>
#include <string>

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
    double total_queries;
    double total_time;
    double min_time;
    double max_time;
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
            QueryParamArray query_param_array;
            all_query_param_arrays.push_back(query_param_array);
        }
        all_query_param_arrays[slot].push_back(query_param);
        
        line_no++;
    }
    
    if(in_file != stdin)
        fclose(in_file);

    // start the workers
    
    num_workers = all_query_param_arrays.size();
    std::vector<pthread_t> threads_array;
    
    for (int i = 0; i < num_workers; ++i) {
        pthread_t thr;
        threads_array.push_back(thr);
        int rc = 0;
        if ( (rc = pthread_create(&threads_array[i], NULL, worker_func, (void*)&i)) ) {
            fprintf(stderr, "failed to create thread num %d, rc: %d\n", i, rc);
            return EXIT_FAILURE;
        }
    }
        
      /* wait for all workers to complete */
    for (int i = 0; i < num_workers; ++i) {
        pthread_join(threads_array[i], NULL);
    }
    
    
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
    
}
