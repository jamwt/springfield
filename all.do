export CFLAGS="-g -Wall -Werror -pedantic -std=gnu99 -O2 -fno-strict-aliasing"
gcc $CFLAGS -o springfield_test springfield.c springfield_test.c -lpthread
