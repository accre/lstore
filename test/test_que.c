/*
   Copyright 2016 Vanderbilt University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/


#include <apr_thread_proc.h>
#include <apr_pools.h>
#include <apr_time.h>
#include <math.h>
#include <stdlib.h>
#include <unistd.h>
#include <tbx/apr_wrapper.h>
#include <tbx/fmttypes.h>
#include <tbx/pipe_helper.h>
#include <tbx/que.h>
#include <tbx/type_malloc.h>

int percentile[] = {1, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 99};

int mode = 0;  //** Which Que to use

typedef struct {
    apr_time_t start;
    apr_time_t end;
    apr_time_t dt;
    int retries;
    int slot;
    int who;
    int times_consumed;
} task_t;

typedef struct {
    apr_thread_t *thread;
    int *pfd;
    tbx_que_t *q;
    apr_time_t dt;
    int ntasks;
    int me;
    int retries;
    task_t *task;
} consumer_t;

typedef struct {
    apr_thread_t *thread;
    int *pfd;
    tbx_que_t *q;
    apr_time_t dt;
    int ntasks;
    int retries;
    task_t *task;
} producer_t;


//************************************************************************************

void *producer_thread(apr_thread_t *th, void *arg)
{
    producer_t *p = (producer_t *)arg;
    task_t *task = p->task;
    task_t *t;
    apr_time_t dt, start;
    int i, n;

    dt = p->dt;

    start = apr_time_now();
    for (i=0; i<p->ntasks; i++) {
        task[i].slot = i;
        task[i].start = apr_time_now();
        task[i].times_consumed = 0;
    again:
        t = task + i;
        if (mode == 0) {
            n = tbx_pipe_put(p->pfd, &t, sizeof(task_t *), dt);
        } else {
            n = tbx_que_put(p->q, &t, dt);
        }
        if (n != 0) {
            task[i].retries++;
            p->retries++;
            goto again;
        }
    }

    p->dt = apr_time_now() - start;
    return(NULL);
}

//************************************************************************************

void *consumer_thread(apr_thread_t *th, void *arg)
{
    consumer_t *c = (consumer_t *)arg;
    task_t *task;
    apr_time_t dt, start;
    int n;

    dt = c->dt;
    c->ntasks = 0;

    start = apr_time_now();
    do {
    again:
        if (mode == 0) {
            n = tbx_pipe_get(c->pfd, &task, sizeof(task_t *), dt);
        } else {
            n = tbx_que_get(c->q, &task, dt);
        }
        if (n != 0) {
            c->retries++;
            goto again;
        } else if (task != NULL) {
            task->end = apr_time_now();
            task->who = c->me;
            task->times_consumed++;
            c->ntasks++;
        }
    } while (task != NULL);

    c->dt = apr_time_now() - start;

    return(NULL);
}

//************************************************************************************

int dt_compare(const void *p1, const void *p2, void *arg)
{
    task_t *t1 = (task_t *)p1;
    task_t *t2 = (task_t *)p2;

    if (t1->dt < t2->dt) {
        return(-1);
    } else if (t1->dt > t2->dt) {
        return(1);
    }

    return(0);
}

//************************************************************************************

int process_results(task_t *task, int ntasks, producer_t *producer, int np, consumer_t *consumer, int nc, apr_time_t runtime)
{
    int err, i, j, k, n, missing, nconsumed, double_cnt;
    double ttime, mean, median, stddev, d;

    err = 0;

    //** Make sure the numbers all add up
    nconsumed = 0;
    for (i=0; i<nc; i++) {
        nconsumed += consumer[i].ntasks;
    }

    if (nconsumed != ntasks) {
        err = 1;
        fprintf(stdout, "ERROR: nconsumed(%d) != ntasks(%d)\n", nconsumed, ntasks);
    }

    //** Generate the dt's
    missing = 0;
    double_cnt = 0;
    ttime = 0;
    for (i=0; i<ntasks; i++) {
        if (task[i].end == 0) {
            fprintf(stdout, "ERROR: MISSING task=%d\n", i);
            missing++;
            err = 1;
        } else if (task[i].times_consumed != 1) {
            fprintf(stdout, "ERROR: DOUBLE counted task=%d times_consumed=%d\n", i, task[i].times_consumed);
            double_cnt++;
            err = 1;        
        } else {
            task[i].dt = task[i].end - task[i].start;
            ttime += task[i].dt;
        }
    }

    if (missing != 0) {
        err = 1;
        fprintf(stdout, "ERROR: missing some tasks! missing=%d\n", missing);
    }
    if (double_cnt != 0) {
        err = 1;
        fprintf(stdout, "ERROR: Double counted some tasks! double_cnt=%d\n", double_cnt);
    }

    //** Sort them
    qsort_r(task, ntasks, sizeof(task_t), dt_compare, NULL);

    //** Print producer and consumer stats
    fprintf(stdout, "-------------- Producer stats (# - dt ntasks rate retries)-------------\n");
    for (i=0; i<np; i++) {
        d = (producer[i].dt*1.0)/producer[i].ntasks;
        fprintf(stdout, "%d - " TT "  %d  %lf %d\n", i, producer[i].dt, producer[i].ntasks, d, producer[i].retries);
    }
    fprintf(stdout, "\n");
    fprintf(stdout, "-------------- Consumer stats (# - dt ntasks rate retries)-------------\n");
    j = k = consumer[0].ntasks;
    for (i=0; i<nc; i++) {
        if (consumer[i].ntasks > k) k = consumer[i].ntasks;
        if (consumer[i].ntasks < j) j = consumer[i].ntasks;
        d =  (consumer[i].ntasks != 0) ? (consumer[i].dt*1.0)/consumer[i].ntasks : 0;
        fprintf(stdout, "%d - " TT "  %d  %lf %d\n", i, consumer[i].dt, consumer[i].ntasks, d, consumer[i].retries);
    }

    n = (j+k)/2;
    d = (double)(k-j)/n*100.0;
    fprintf(stdout, "\nntask range: %d - %d (%4.2lf%%)  Avg: %d \n", j, k, d, n);
    fprintf(stdout, "\n");

    //** print percentiles
    fprintf(stdout, "--------Printing Percentiles--------\n");
    n = sizeof(percentile)/sizeof(int);
    for (i=0; i<n; i++) {
        j = (ntasks * percentile[i])/100;
        fprintf(stdout, "T[%d]=" TT "(slot=%d)\n", percentile[i], task[j].dt, task[j].slot);
    }
    fprintf(stdout, "\n");

    //** Print mean, stddev, median
    mean = ttime / ntasks;
    stddev = 0;
    for (i=0; i<ntasks; i++) {
        d  = task[i].dt - mean;
        stddev += d*d;
    }
    stddev = sqrt(stddev/ntasks);
    median = task[ntasks/2].dt;
    fprintf(stdout, "Range(us): " TT "(%d) - " TT "(%d)\n", task[0].dt, task[0].slot, task[ntasks-1].dt, task[ntasks-1].slot);
    fprintf(stdout, "mean(us):   %lf\n", mean);
    fprintf(stdout, "median(us): %lf\n", median);
    fprintf(stdout, "stddev(us): %lf\n", stddev);
    d = ntasks/(1.0*runtime) * APR_USEC_PER_SEC;
    fprintf(stdout, "task/s: %lf\n", d);
    d = (1.0*runtime) / APR_USEC_PER_SEC;
    fprintf(stdout, "Runtime(s): %lf\n", d);

    if (err == 0) {
        fprintf(stdout, "SUCCESS!\n");
    } else {
        fprintf(stdout, "ERROR: err=%d\n", err);
    }
    return(err);
}

//************************************************************************************


int main(int argc, char **argv)
{
    int np, nc, ntasks, i, start_option, j, k, n;
    int pfd[2], n_slots;
    tbx_que_t *q;
    apr_status_t dummy;
    apr_time_t runtime, dt;
    task_t *task, *t;
    consumer_t *consumer;
    producer_t *producer;
    apr_pool_t *mpool;

    np = 1;
    nc = 1;
    ntasks = 1000;

    if (argc == 1) {
        printf("test_que --pipe|--que n_slots --dt dt --np n_producers --nc n_consumers --ntasks ntasks\n");
        printf("    --pipe   Use the system pipe functions for interprocess communication(default mode)\n");
        printf("    --que n_slots      Use the array queue functions for interprocess communication\n");
        printf("    --dt dt            Max time to wait for a task(us).\n");
        printf("    --np n_producers   Number of producer threads. Default is %d.\n", np);
        printf("    --nc n_consumers   Number of consumer threads. Defaults is %d.\n", nc);
        printf("    --ntasks ntasks    Total number of tasks to process. Default is %d\n", ntasks);
        printf("\n");
     }

    dt = apr_time_from_sec(1);
    n_slots = 100;

    i = 1;
    do {
        start_option = i;
        if (strcmp(argv[i], "--pipe") == 0) {
            mode = 0;
            i++;
        } else if (strcmp(argv[i], "--que") == 0) {
            mode = 1;
            i++;
            n_slots = atol(argv[i]);
            i++;
        } else if (strcmp(argv[i], "--dt") == 0) {
            i++;
            dt = atol(argv[i]);
            i++;
        } else if (strcmp(argv[i], "--nc") == 0) {
            i++;
            nc = atol(argv[i]);
            i++;
        } else if (strcmp(argv[i], "--np") == 0) {
            i++;
            np = atol(argv[i]);
            i++;
        } else if (strcmp(argv[i], "--ntasks") == 0) {
            i++;
            ntasks = atol(argv[i]);
            i++;
        }
    } while  ((start_option < i) && (i<argc));


    //** Allocate all the space
    tbx_type_malloc_clear(task, task_t, ntasks);
    tbx_type_malloc_clear(consumer, consumer_t, nc);
    tbx_type_malloc_clear(producer, producer_t, np);

    //** Figure out the work distribution
    j = ntasks / np;
    k = ntasks % np;
    n = 0;
    for (i=0; i<np; i++) {
        producer[i].task = task + n;
        producer[i].ntasks = (i<k) ? j+1 : j;
        n += producer[i].ntasks;
    }

    apr_pool_create(&mpool, NULL);

    q = NULL;
    pfd[0] = pfd[1] = -1;
    if (mode == 0) {
        fprintf(stderr, "Using pipes for communication\n");
        tbx_pipe_open(pfd);
    } else {
        fprintf(stderr, "Using array que for communication with %d slots\n", n_slots);
        q = tbx_que_create(n_slots, sizeof(task_t *));
    }

    runtime = apr_time_now();

    //** Launch the consumers
    for (i=0; i<nc; i++) {
        consumer[i].task = task;
        consumer[i].dt = dt;
        consumer[i].me = i;
        consumer[i].pfd = pfd;
        consumer[i].q = q;
        tbx_thread_create_assert(&(consumer[i].thread), NULL, consumer_thread, consumer + i, mpool);
    }

    //** Launch the producers
    for (i=0; i<np; i++) {
        producer[i].dt = dt;
        producer[i].pfd = pfd;
        producer[i].q = q;
        tbx_thread_create_assert(&(producer[i].thread), NULL, producer_thread, producer + i, mpool);
    }

    //** Wait for everything to complete
    for (i=0; i<np; i++) {
        apr_thread_join(&dummy, producer[i].thread);
    }
    t = NULL;  //** Tell the consumeers to shut down
    for (i=0; i<nc; i++) {
        if (mode == 0) {
            tbx_pipe_put(pfd, &t, sizeof(task_t *), apr_time_from_sec(30));
        } else {
            tbx_que_put(q, &t, apr_time_from_sec(30));
        }
    }
    for (i=0; i<nc; i++) {
        apr_thread_join(&dummy, consumer[i].thread);
    }
    runtime = apr_time_now() - runtime;

    if (mode == 0) {
        tbx_pipe_close(pfd);
    } else {
        tbx_que_destroy(q);
    }

    //** Process the results
    n = process_results(task, ntasks, producer, np, consumer, nc, runtime);

    //** Cleanup
    free(task);
    free(producer);
    free(consumer);
    return(n);
}