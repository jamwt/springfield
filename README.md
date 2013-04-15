Springfield
===========

Springfield is "big rolla".

It takes the rolla storage engine design and scales it up
for use in larger 64-bit systems with millions of keys
and data files on the order of GBs+.

Unlike rolla, it remember buckets counts in db files,
so you don't need to walk on eggshells making sure you
use the same `NUM_BUCKETS` every time with a given db
file.

Springfield is fully thread-safe.  In fact,
You can also compress and "upgrade" the db to
higher bucket count while remaining online to get your 
performance back when the keyspace grows.  
(use `springfield_seek_average()`
to get back a heuristic that can help inform when it's time
to close/compress/resize)

Springfield also uses CRC sums to validate data
integrity of keys/values on disk.
