#include "atomic.h"
#include <stdio.h>

int
main() {
	atomic_ptr v = ATOMIC_VAR_INIT(0);

	printf("%d\n", atomic_ptr_cas(&v, NULL, NULL));

	return 0;
}