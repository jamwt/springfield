Springfield
===========

Springfield is "big rolla".

It takes the rolla storage engine design and scales it up
for use in larger 64-bit systems with millions of keys
and data files on the order of GBs+.  Like rolla, it is
designed assuming it runs on ~100us random access
times of solid state disks.

Unlike rolla, it remembers buckets counts in db files,
so you don't need to walk on eggshells making sure you
use the same `NUM_BUCKETS` every time with a given db
file.

Springfield is fully thread-safe.  In fact,
You can also compact and "upgrade" the db to
higher bucket count while remaining online to get your
performance back when the keyspace grows.  And, on SSDs,
when the page cache is not large enough to cover you,
parallel gets on separate threads speed things up nearly
linearly.

Springfield also uses CRC sums to validate data
integrity of keys/values on disk.

Status
------

Springfield is alpha.  There is at least one known
bug/race in online compaction, but everything else
seems to work fine.

Still, don't use it in production, yet!

Performance
-----------

It is pretty fast on SSDs.  It outperforms
Tokyo Cabinet's TCHDB on most workloads, and
often by a wide margin as key counts and db
sizes go up.
