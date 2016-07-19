#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include "pcg_variants.h"

// We generate 10bn+ (> 2^32) ids so we need good RNG
// We'd like fast creation.
// Needs to be .csv so the same data can be loaded into different systems.
// Hence:
// Download PCG from http://www.pcg-random.org
// Save this file into sample directory: tableGen.c
// Add tableGen to Makefile and make
// Or use the x86 64-bit executable 'tableGen' built on Ubuntu

int main(int argc, char** argv)
{
  if (argc != 3) {
    printf("Usage: ./tableGen X|Y nrow\n");
    exit(1);
  }
  char *tableName = argv[1];
  pcg64_random_t rng;
  if (strcmp(tableName, "X") == 0) {
    pcg64_srandom_r(&rng, 42u, 54u);
  } else if (strcmp(tableName, "Y") == 0) {
    pcg64_srandom_r(&rng, 74u, 41u);
  } else {
    printf("Second argument must be X or Y\n");
    exit(1);
  }
  double d;
  sscanf(argv[2], "%lf", &d);
  uint64_t nrow = (uint64_t)d;
  printf("KEY,%s2\n", tableName);
  for (uint64_t i=0; i<nrow; i++) {
    printf("%"PRIu64",%"PRId64"\n",
      pcg64_boundedrand_r(&rng, nrow)+1,                 // random id in the uniform range [1,nrow], some dups but not many
      (int64_t)pcg64_boundedrand_r(&rng, 2*nrow+1)-nrow  // any old pretty much unique data so we must move it around
    );
  }
  return 0;
}

