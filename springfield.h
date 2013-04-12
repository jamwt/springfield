#ifndef SPRINGFIELD_H
#define SPRINGFIELD_H

#include <inttypes.h>

/* This is your database, friend. */
typedef struct springfield_t springfield_t;

/* Create a database (in a single file) at `path`.
   If `path` does not exist, it will be created;
   otherwise, it will be loaded. */
springfield_t * springfield_create(char *path, uint32_t num_buckets);

/* Force the database to be sync'd to disk (msync) */
void springfield_sync(springfield_t *r);

/* Get the average number of seeks on a record hit in the
   last 100 fetches */
double springfield_seek_average(springfield_t *r);

/* Close the database; compress=1 means rewrite the database
   to eliminate redundant values for each single key */
void springfield_close(springfield_t *r, int compress,
    uint32_t num_buckets);

/* Set `key` to byte array `val` of `vlen` bytes; you still
   own key and val, they are not retained */
void springfield_set(springfield_t *r, char *key, uint8_t *val, uint32_t vlen);

/* Get the value for `key`, which will be `*len` bytes long.
   NULL will be returned if the key is not found.  Otherwise,
   a value will be returned to you that is allocated on the heap.
   You own it, you must free() it eventually. */
uint8_t * springfield_get(springfield_t *r, char *key, uint32_t *len);

/* Remove the value `key` from the database.  Harmless NOOP
   if `key` does not exist */
void springfield_del(springfield_t *r, char *key);

/* Iterate over all keys in the database.  See the note in the
   README.md about caveats associated with iteration and mutation */
typedef void(*springfield_iter_cb) (springfield_t *r, char *key, uint8_t *val, uint32_t length, void *passthrough);
void springfield_iter(springfield_t *r, springfield_iter_cb cb, void *passthrough);

#endif /* SPRINGFIELD_H */
