/* Disk layout:

24 byte header:

|      crc      |  ver  |  kl   |
| 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 |

|      vlen     |     flags     |
| 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 |

|   previous_offset_in_bucket   |
| 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 |

<kl-octet key>
<vlen-octet value>

*/
#include "springfield.h"

#include <assert.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

typedef struct springfield_header_v1 {
    uint32_t crc;
    uint16_t version;
    uint16_t klen;

    uint32_t vlen;
    uint32_t flags; /* snappy compressed? */

    uint64_t last;
} springfield_header_v1;


struct springfield_t {
    uint32_t num_buckets;
    uint64_t *offsets;
    int mapfd;
    char *path;
    uint8_t *map;
    uint64_t mmap_alloc;
    uint64_t eof;
    uint32_t seeks[100];
    int seek_pos;
};

#define HEADER_SIZE (sizeof(springfield_header_v1))
#define HEADER_SIZE_MINUS_CRC (sizeof(springfield_header_v1) - 4)
#define MMAP_OVERFLOW (128 * 1024)
#define NO_BACKTRACE (~((uint64_t)0) )
#define MAX_KLEN ((uint16_t)0xffff)
#define MAX_VLEN (((uint32_t)0xffffffff) - MAX_KLEN - HEADER_SIZE)

static uint32_t jenkins_one_at_a_time_hash(char *key, size_t len);
static uint32_t crc32(uint32_t crc, uint8_t *buf, int len);

#define hash jenkins_one_at_a_time_hash

static uint64_t springfield_index_lookup(springfield_t *r, char *key) {
    uint32_t fh = hash(key, strlen(key)) % r->num_buckets;
    return r->offsets[fh];
}

static uint64_t springfield_index_keyval(springfield_t *r, char *key, uint64_t off) {
    uint32_t fh = hash(key, strlen(key)) % r->num_buckets;

    uint64_t last = r->offsets[fh];
    r->offsets[fh] = off;

    return last;
}

static void springfield_load(springfield_t *r) {

    struct stat st;

    r->mapfd = open(r->path,
            O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    assert(r->mapfd > -1);

    int s = fstat(r->mapfd, &st);
    if (!s) {
        r->eof = st.st_size >= 4 ? st.st_size : 0;
    }

    r->map = NULL;

    if (!r->eof) {
        r->offsets = malloc(r->num_buckets * sizeof(uint64_t));
        memset(r->offsets, 0xff, r->num_buckets * sizeof(uint64_t));

    } else {
        r->mmap_alloc = r->eof;
        r->map = (uint8_t *)mmap(
            NULL, r->mmap_alloc, PROT_READ, MAP_PRIVATE, r->mapfd, 0);
        assert(r->map);

        uint8_t *p = r->map;

        r->num_buckets = *(uint32_t *)p;
        r->offsets = malloc(r->num_buckets * sizeof(uint64_t));
        memset(r->offsets, 0xff, r->num_buckets * sizeof(uint64_t));

        uint64_t off = 4;
        p += 4;

        while (1) {
            if (off + 8 > r->eof) {
                r->eof = off;
                break;
            }
            springfield_header_v1 *h = (springfield_header_v1 *)p;
            if (h->version == 0) {
                r->eof = off;
                break;
            }
            assert(h->version == 1);
            if (off + HEADER_SIZE > r->eof) {
                r->eof = off;
                break;
            }
            if (h->klen == 0) {
                r->eof = off;
                break;
            }
            assert(h->vlen <= MAX_VLEN);
            uint32_t jump = h->vlen + h->klen + HEADER_SIZE;
            if (off + jump > r->eof) {
                r->eof = off;
                break;
            }

            /* Check CRC32 */
            if (crc32(0, p + 4, HEADER_SIZE_MINUS_CRC + h->klen + h->vlen) != h->crc) {
                r->eof = off;
                break;
            }

            char *key = (char *)(p + HEADER_SIZE);

            uint64_t prev = springfield_index_keyval(r, key, off);
            assert(prev == h->last);

            off += jump;
            p += jump;
        }

        munmap(r->map, r->mmap_alloc);
    }

    r->mmap_alloc = r->eof + MMAP_OVERFLOW;
    s = ftruncate(r->mapfd, (off_t)r->mmap_alloc);
    assert(!s);

    r->map = (uint8_t *)mmap(
        NULL, r->mmap_alloc, PROT_READ | PROT_WRITE, MAP_SHARED, r->mapfd, 0);

    uint32_t buckets_on_record = *(uint32_t *)r->map;
    if (buckets_on_record) {
        assert(buckets_on_record == r->num_buckets);
    } else {
        *(uint32_t *)r->map = r->num_buckets;
        assert(r->eof == 0);
        r->eof = 4;
    }

    assert(r->map);
}

springfield_t * springfield_create(char *path, uint32_t num_buckets) {
    assert(sizeof(void *) == 8); // Springfield needs 64-bit system
    springfield_t *r = calloc(1, sizeof(springfield_t));
    r->num_buckets = num_buckets;
    r->path = malloc(strlen(path) + 1);
    strcpy(r->path, path);
    r->mmap_alloc = 0;

    springfield_load(r);

    return r;
}

uint8_t * springfield_get(springfield_t *r, char *key, uint32_t *len) {
    uint64_t off = springfield_index_lookup(r, key);

    int seeks = 0;
    while (off != NO_BACKTRACE) {
        ++seeks;
        uint8_t *p = &r->map[off];
        springfield_header_v1 *h = (springfield_header_v1 *)p;
        if (!strncmp((char *)(p + HEADER_SIZE), key, h->klen)) {
            r->seeks[r->seek_pos] = seeks;
            if (++r->seek_pos == 100) {
                r->seek_pos = 0;
            }
            if (h->vlen == 0) {
                return NULL;
            }
            uint8_t *res = malloc(h->vlen);
            *len = h->vlen;
            memmove(res, p + HEADER_SIZE + h->klen, h->vlen);
            return res;
        }
        off = h->last;
    }

    return NULL;
}

double springfield_seek_average(springfield_t *r) {
    double tot = 0;
    int i;
    for(i=0; i < 100; i++) {
        tot += r->seeks[i];
    }
    return tot / 100.0;
}

void springfield_sync(springfield_t *r) {
    int s = msync(r->map, r->mmap_alloc, MS_SYNC);
    assert(!s);
}

void springfield_set(springfield_t *r, char *key, uint8_t *val, uint32_t vlen) {
    uint8_t klen = strlen(key) + 1;
    assert(klen < MAX_KLEN);
    assert(vlen < MAX_VLEN);

    uint32_t step = HEADER_SIZE + klen + vlen;
    springfield_header_v1 h = {0};
    h.klen = klen;
    h.vlen = vlen;
    h.version = 1;

    if (r->eof + step > r->mmap_alloc) {
        msync(r->map, r->mmap_alloc, MS_SYNC);
        int s = munmap(r->map, r->mmap_alloc);
        assert(!s);
        uint64_t new_size = r->mmap_alloc + ((r->eof + step) * 2);
        if (new_size > UINT_MAX) {
            assert(r->mmap_alloc != UINT_MAX);
            new_size = UINT_MAX;
        }
        r->mmap_alloc = new_size;
        s = ftruncate(r->mapfd, (off_t)r->mmap_alloc);
        assert(!s);
        r->map = (uint8_t *)mmap(
            NULL, r->mmap_alloc, PROT_READ | PROT_WRITE, MAP_SHARED, r->mapfd, 0);
        /* TODO mremap() on linux */
    }

    h.last = springfield_index_keyval(r, key, r->eof);
    uint8_t *p = &r->map[r->eof];

    springfield_header_v1 *ph = (springfield_header_v1 *)p;

    *ph = h;

    memmove(p + HEADER_SIZE, key, klen);
    if (vlen)
        memmove(p + HEADER_SIZE + klen, val, vlen);

    ph->crc = crc32(0, p + 4, HEADER_SIZE_MINUS_CRC + klen + vlen);

    r->eof += step;
}

void springfield_del(springfield_t *r, char *key) {
    springfield_set(r, key, NULL, 0);
}

void springfield_iter(springfield_t *r, springfield_iter_cb cb, void *passthrough) {
    int i;

    int sl = 10 * 1024;
    char *s = realloc(NULL, sl);
    for (i = 0; i < r->num_buckets; i++) {
        uint64_t search_off = 0;
        s[0] = 0;
        uint64_t off = r->offsets[i];
        char buf[MAX_KLEN + 2];
        buf[0] = 1;
        while (off != NO_BACKTRACE) {
            springfield_header_v1 *h = (springfield_header_v1 *)(r->map + off);
            char *key = (char *)(r->map + off + HEADER_SIZE);
            strcpy(buf + 1, key);
            buf[h->klen] = 1;
            buf[h->klen + 1] = 0;
            uint32_t skey_len = h->klen + 2;
            if (!strstr(s, buf)) {
                /* not found */
                if (h->vlen) {
                    uint8_t *val = (uint8_t *)(r->map + off + h->klen + HEADER_SIZE);
                    cb(r, key, val, h->vlen, passthrough);
                }
                if (search_off + skey_len >= sl) {
                    sl *= 2;
                    s = realloc(s, sl);
                }
                strcpy(s + search_off, buf);
                search_off += skey_len - 1; /* trailing \0 */
            }

            off = h->last;
        }
    }

    free(s);
}

static void springfield_rewrite_cb(springfield_t *r, char *key, uint8_t *data, uint32_t length, void *pass) {
    springfield_t *new = (springfield_t *)pass;
    springfield_set(new, key, data, length);
}

void springfield_close(springfield_t *r, int compress, uint32_t num_buckets) {
    char path[1200] = {0};
    if (compress) {
        assert(strlen(r->path) < 1100);
        strcat(path, r->path);
        strcat(path, ".springfield_rewrite");

        springfield_t *tmp = springfield_create(path, num_buckets ?
            num_buckets : r->num_buckets);
        springfield_iter(r, springfield_rewrite_cb, (void *)tmp);
        springfield_close(tmp, 0, 0);
    }

    munmap(r->map, r->mmap_alloc);
    close(r->mapfd);

    if (compress) {
        rename(path, r->path);
    }

    free(r->path);
    free(r->offsets);
    free(r);
}

/* From Bob Jenkins/Dr. Dobbs */
static uint32_t jenkins_one_at_a_time_hash(char *key, size_t len)
{
    uint32_t hash, i;
    for(hash = i = 0; i < len; ++i)
    {
        hash += key[i];
        hash += (hash << 10);
        hash ^= (hash >> 6);
    }
    hash += (hash << 3);
    hash ^= (hash >> 11);
    hash += (hash << 15);
    return hash;
}

/* -- CRC32 courtesy of zlib -- */

/* Note: modified by springfield project for style and
   to remove dependencies on zlib.h 
*/

/*
  Copyright (C) 1995-2010 Jean-loup Gailly and Mark Adler

  This software is provided 'as-is', without any express or implied
  warranty.  In no event will the authors be held liable for any damages
  arising from the use of this software.

  Permission is granted to anyone to use this software for any purpose,
  including commercial applications, and to alter it and redistribute it
  freely, subject to the following restrictions:

  1. The origin of this software must not be misrepresented; you must not
     claim that you wrote the original software. If you use this software
     in a product, an acknowledgment in the product documentation would be
     appreciated but is not required.
  2. Altered source versions must be plainly marked as such, and must not be
     misrepresented as being the original software.
  3. This notice may not be removed or altered from any source distribution.

  Jean-loup Gailly        Mark Adler
  jloup@gzip.org          madler@alumni.caltech.edu

*/

/* ========================================================================
 * Table of CRC-32's of all single-byte values (made by make_crc_table)
 */
static const uint32_t crc_table[256] = {
  0x00000000L, 0x77073096L, 0xee0e612cL, 0x990951baL, 0x076dc419L,
  0x706af48fL, 0xe963a535L, 0x9e6495a3L, 0x0edb8832L, 0x79dcb8a4L,
  0xe0d5e91eL, 0x97d2d988L, 0x09b64c2bL, 0x7eb17cbdL, 0xe7b82d07L,
  0x90bf1d91L, 0x1db71064L, 0x6ab020f2L, 0xf3b97148L, 0x84be41deL,
  0x1adad47dL, 0x6ddde4ebL, 0xf4d4b551L, 0x83d385c7L, 0x136c9856L,
  0x646ba8c0L, 0xfd62f97aL, 0x8a65c9ecL, 0x14015c4fL, 0x63066cd9L,
  0xfa0f3d63L, 0x8d080df5L, 0x3b6e20c8L, 0x4c69105eL, 0xd56041e4L,
  0xa2677172L, 0x3c03e4d1L, 0x4b04d447L, 0xd20d85fdL, 0xa50ab56bL,
  0x35b5a8faL, 0x42b2986cL, 0xdbbbc9d6L, 0xacbcf940L, 0x32d86ce3L,
  0x45df5c75L, 0xdcd60dcfL, 0xabd13d59L, 0x26d930acL, 0x51de003aL,
  0xc8d75180L, 0xbfd06116L, 0x21b4f4b5L, 0x56b3c423L, 0xcfba9599L,
  0xb8bda50fL, 0x2802b89eL, 0x5f058808L, 0xc60cd9b2L, 0xb10be924L,
  0x2f6f7c87L, 0x58684c11L, 0xc1611dabL, 0xb6662d3dL, 0x76dc4190L,
  0x01db7106L, 0x98d220bcL, 0xefd5102aL, 0x71b18589L, 0x06b6b51fL,
  0x9fbfe4a5L, 0xe8b8d433L, 0x7807c9a2L, 0x0f00f934L, 0x9609a88eL,
  0xe10e9818L, 0x7f6a0dbbL, 0x086d3d2dL, 0x91646c97L, 0xe6635c01L,
  0x6b6b51f4L, 0x1c6c6162L, 0x856530d8L, 0xf262004eL, 0x6c0695edL,
  0x1b01a57bL, 0x8208f4c1L, 0xf50fc457L, 0x65b0d9c6L, 0x12b7e950L,
  0x8bbeb8eaL, 0xfcb9887cL, 0x62dd1ddfL, 0x15da2d49L, 0x8cd37cf3L,
  0xfbd44c65L, 0x4db26158L, 0x3ab551ceL, 0xa3bc0074L, 0xd4bb30e2L,
  0x4adfa541L, 0x3dd895d7L, 0xa4d1c46dL, 0xd3d6f4fbL, 0x4369e96aL,
  0x346ed9fcL, 0xad678846L, 0xda60b8d0L, 0x44042d73L, 0x33031de5L,
  0xaa0a4c5fL, 0xdd0d7cc9L, 0x5005713cL, 0x270241aaL, 0xbe0b1010L,
  0xc90c2086L, 0x5768b525L, 0x206f85b3L, 0xb966d409L, 0xce61e49fL,
  0x5edef90eL, 0x29d9c998L, 0xb0d09822L, 0xc7d7a8b4L, 0x59b33d17L,
  0x2eb40d81L, 0xb7bd5c3bL, 0xc0ba6cadL, 0xedb88320L, 0x9abfb3b6L,
  0x03b6e20cL, 0x74b1d29aL, 0xead54739L, 0x9dd277afL, 0x04db2615L,
  0x73dc1683L, 0xe3630b12L, 0x94643b84L, 0x0d6d6a3eL, 0x7a6a5aa8L,
  0xe40ecf0bL, 0x9309ff9dL, 0x0a00ae27L, 0x7d079eb1L, 0xf00f9344L,
  0x8708a3d2L, 0x1e01f268L, 0x6906c2feL, 0xf762575dL, 0x806567cbL,
  0x196c3671L, 0x6e6b06e7L, 0xfed41b76L, 0x89d32be0L, 0x10da7a5aL,
  0x67dd4accL, 0xf9b9df6fL, 0x8ebeeff9L, 0x17b7be43L, 0x60b08ed5L,
  0xd6d6a3e8L, 0xa1d1937eL, 0x38d8c2c4L, 0x4fdff252L, 0xd1bb67f1L,
  0xa6bc5767L, 0x3fb506ddL, 0x48b2364bL, 0xd80d2bdaL, 0xaf0a1b4cL,
  0x36034af6L, 0x41047a60L, 0xdf60efc3L, 0xa867df55L, 0x316e8eefL,
  0x4669be79L, 0xcb61b38cL, 0xbc66831aL, 0x256fd2a0L, 0x5268e236L,
  0xcc0c7795L, 0xbb0b4703L, 0x220216b9L, 0x5505262fL, 0xc5ba3bbeL,
  0xb2bd0b28L, 0x2bb45a92L, 0x5cb36a04L, 0xc2d7ffa7L, 0xb5d0cf31L,
  0x2cd99e8bL, 0x5bdeae1dL, 0x9b64c2b0L, 0xec63f226L, 0x756aa39cL,
  0x026d930aL, 0x9c0906a9L, 0xeb0e363fL, 0x72076785L, 0x05005713L,
  0x95bf4a82L, 0xe2b87a14L, 0x7bb12baeL, 0x0cb61b38L, 0x92d28e9bL,
  0xe5d5be0dL, 0x7cdcefb7L, 0x0bdbdf21L, 0x86d3d2d4L, 0xf1d4e242L,
  0x68ddb3f8L, 0x1fda836eL, 0x81be16cdL, 0xf6b9265bL, 0x6fb077e1L,
  0x18b74777L, 0x88085ae6L, 0xff0f6a70L, 0x66063bcaL, 0x11010b5cL,
  0x8f659effL, 0xf862ae69L, 0x616bffd3L, 0x166ccf45L, 0xa00ae278L,
  0xd70dd2eeL, 0x4e048354L, 0x3903b3c2L, 0xa7672661L, 0xd06016f7L,
  0x4969474dL, 0x3e6e77dbL, 0xaed16a4aL, 0xd9d65adcL, 0x40df0b66L,
  0x37d83bf0L, 0xa9bcae53L, 0xdebb9ec5L, 0x47b2cf7fL, 0x30b5ffe9L,
  0xbdbdf21cL, 0xcabac28aL, 0x53b39330L, 0x24b4a3a6L, 0xbad03605L,
  0xcdd70693L, 0x54de5729L, 0x23d967bfL, 0xb3667a2eL, 0xc4614ab8L,
  0x5d681b02L, 0x2a6f2b94L, 0xb40bbe37L, 0xc30c8ea1L, 0x5a05df1bL,
  0x2d02ef8dL
};

/* ========================================================================= */
#define DO1(buf) crc = crc_table[((int)crc ^ (*buf++)) & 0xff] ^ (crc >> 8);
#define DO2(buf)  DO1(buf); DO1(buf);
#define DO4(buf)  DO2(buf); DO2(buf);
#define DO8(buf)  DO4(buf); DO4(buf);

/* ========================================================================= */
static uint32_t crc32(uint32_t crc, uint8_t *buf, int len) {
    if (buf == NULL) return 0L;
    crc = crc ^ 0xffffffffL;
    while (len >= 8)
    {
      DO8(buf);
      len -= 8;
    }
    if (len) do {
      DO1(buf);
    } while (--len);
    return crc ^ 0xffffffffL;
}
