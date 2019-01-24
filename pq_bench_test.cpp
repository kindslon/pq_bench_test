/*
 select time_bucket('1 minute', ts), sum(usage) 
 from cpu_usage 
 where ts between '2017-01-01 00:00:00' and '2017-01-01 00:59:00' 
 group by 1 order by 1 limit 200;
 * 
 * pg_config --includedir
 * /usr/lib/x86_64-linux-gnu/

 */
using namespace std;

/*
 * 
 */
int main(int argc, char** argv) {
    printf("hi there!\n");

    return 0;
}

