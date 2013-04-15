#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>

#include "springfield.h"

#define COUNT 1000000
#define DCOUNT ((double)COUNT)
#define BUCKETS (1024 * 120)

double doublenow() {
    struct timeval tv;
    gettimeofday(&tv, NULL);

    return (double)tv.tv_sec
        + (((double)tv.tv_usec) / 1000000.0);
}

void *do_compact(void *d) {
    double start, final;
    springfield_t *db = (springfield_t *)d;

    printf("-- start compact --\n");
    /* Online compact */
    start = doublenow();
    springfield_compact(db, BUCKETS * 4);
    final = doublenow();
    printf("compact took %.3f (%.3f/s)\n",
    final - start, DCOUNT / (final - start));

    return NULL;
}

int main() {
    double start;
    printf("-- load --\n");
    start = doublenow();
    springfield_t *db = springfield_create("db", BUCKETS);
    printf("load took %.3f\n",
    doublenow() - start);

    pthread_t t;
    pthread_create(&t, NULL, do_compact, db);

    int i;
    char buf2[8] = {0};
    printf("-- write --\n");

    start = doublenow();
    for (i=0; i < COUNT; i++) {
        snprintf(buf2, 8, "%d", i % 2 ? i : 4);
        springfield_set(db, buf2, (uint8_t *)buf2, 8);
    }
    double final;
    final = doublenow();
    printf("write took %.3f (%.3f/s)\n",
    final - start, DCOUNT / (final - start));
    sleep(3);


    printf("-- read --\n");
    uint32_t sz;
    start = doublenow();
    for (i=0; i < COUNT; i++) {
        snprintf(buf2, 8, "%d", i % 2 ? i : 4);
        char *p = (char *)springfield_get(db, buf2, &sz);
        assert(p && !strcmp(buf2, p));
        free(p);
        if (i % 100000 == 0) {
            printf("%d\n", i);
            printf("current seek average: %.1f\n",
            springfield_seek_average(db));
        }
    }
    final = doublenow();
    printf("read took %.3f (%.3f/s)\n",
    final - start, DCOUNT / (final - start));
    sleep(3);


    printf("-- read --\n");
    start = doublenow();
    for (i=0; i < COUNT; i++) {
        snprintf(buf2, 8, "%d", i % 2 ? i : 4);
        char *p = (char *)springfield_get(db, buf2, &sz);
        assert(p && !strcmp(buf2, p));
        free(p);
        if (i % 100000 == 0) {
            printf("%d\n", i);
            printf("current seek average: %.1f\n",
            springfield_seek_average(db));
        }
    }
    final = doublenow();
    printf("read took %.3f (%.3f/s)\n",
    final - start, DCOUNT / (final - start));
    sleep(3);


    snprintf(buf2, 8, "%d", 4);
    char *p = (char *)springfield_get(db, buf2, &sz);
    assert(p);
    free(p);
    springfield_del(db, buf2);
    p = (char *)springfield_get(db, buf2, &sz);
    assert(!p);

    springfield_close(db);

    void *vres;
    pthread_join(t, &vres);

    return 0;
}
