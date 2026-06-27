#include <check.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include "src/lua-seri.h"

START_TEST(test_wb_push_buffer_bounds)
{
    // Invariant: Buffer writes never exceed declared block boundaries
    struct write_block wb;
    char buffer[BLOCK_SIZE * 2];
    
    // Initialize write block
    memset(&wb, 0, sizeof(wb));
    wb.current = blk_alloc();
    wb.ptr = 0;
    wb.len = 0;
    
    // Payload 1: Exact exploit case - negative size causing integer overflow
    int exploit_sz = -1;
    
    // Payload 2: Boundary case - size equal to remaining space
    int boundary_sz = BLOCK_SIZE - 10;
    
    // Payload 3: Valid input - small size
    int valid_sz = 16;
    
    // Payload 4: Oversized input exceeding block size
    int oversized_sz = BLOCK_SIZE + 100;
    
    int test_sizes[] = {exploit_sz, boundary_sz, valid_sz, oversized_sz};
    int num_tests = sizeof(test_sizes) / sizeof(test_sizes[0]);
    
    for (int i = 0; i < num_tests; i++) {
        // Reset write block for each test
        wb.current = blk_alloc();
        wb.ptr = 0;
        wb.len = 0;
        
        // Fill test buffer with pattern
        memset(buffer, 0xAA, sizeof(buffer));
        
        // Call the actual vulnerable function
        wb_push(&wb, buffer, test_sizes[i]);
        
        // Check that we didn't corrupt memory by verifying
        // the write block structure is still valid
        ck_assert_ptr_nonnull(wb.current);
        ck_assert_int_ge(wb.ptr, 0);
        ck_assert_int_le(wb.ptr, BLOCK_SIZE);
        ck_assert_int_ge(wb.len, 0);
    }
}
END_TEST

Suite *security_suite(void)
{
    Suite *s;
    TCase *tc_core;

    s = suite_create("Security");
    tc_core = tcase_create("Core");

    tcase_add_test(tc_core, test_wb_push_buffer_bounds);
    suite_add_tcase(s, tc_core);

    return s;
}

int main(void)
{
    int number_failed;
    Suite *s;
    SRunner *sr;

    s = security_suite();
    sr = srunner_create(s);

    srunner_run_all(sr, CK_NORMAL);
    number_failed = srunner_ntests_failed(sr);
    srunner_free(sr);

    return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}