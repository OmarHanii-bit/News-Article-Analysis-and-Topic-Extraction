"""
Unified Test Runner for News Article Big Data Pipeline
======================================================
Component: Quality Assurance & Engineering Verification
Author: David Kim (QA, Benchmarking & Presentation Lead)
"""

import unittest
import sys
import os
import time

# The suite has grown over time; 53 is the historical baseline. New modules add
# tests on top of it, but losing/dropping any of the baseline must fail CI.
MIN_EXPECTED_TESTS = 53

def run_all_tests():
    print("=" * 80)
    print("   AUTOMATED TEST SUITE: NEWS ARTICLE BIG DATA PIPELINE")
    print("   Testing MapReduce | PySpark Schema | MongoDB NoSQL | Storage Integrity")
    print("=" * 80)
    
    start_time = time.time()
    
    # Discover and run all test modules in the tests/ directory
    loader = unittest.TestLoader()
    suite = loader.discover(start_dir=os.path.dirname(os.path.abspath(__file__)), pattern="test_*.py")
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    elapsed = time.time() - start_time
    print("\n" + "=" * 80)
    print("Test Run Summary:")
    print(f"  - Tests Executed: {result.testsRun}")
    print(f"  - Failures:       {len(result.failures)}")
    print(f"  - Errors:         {len(result.errors)}")
    print(f"  - Time Elapsed:   {elapsed:.2f} seconds")
    print("=" * 80)

    # Guard against discovery regressions on Windows / CI: never accept a run
    # with fewer tests than the established baseline (53).
    if result.testsRun < MIN_EXPECTED_TESTS:
        print(f"[FAIL] Only {result.testsRun} tests ran; expected at least "
              f"{MIN_EXPECTED_TESTS}. Discovery may have dropped a module.")
        return 1

    if result.wasSuccessful():
        print("[PASS] ALL TESTS PASSED SUCCESSFULLY! Codebase is ready for grading defense.")
        return 0
    else:
        print("[FAIL] SOME TESTS FAILED. Review tracebacks above.")
        return 1

if __name__ == "__main__":
    exit_code = run_all_tests()
    sys.exit(exit_code)
