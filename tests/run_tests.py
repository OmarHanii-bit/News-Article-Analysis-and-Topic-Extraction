"""
Unified Test Runner for News Article Big Data Pipeline
======================================================
Component: Quality Assurance & Engineering Verification
Author: Mazen Salem (QA, Benchmarking & Presentation Lead)
"""

import unittest
import sys
import os
import time

# The full verification suite comprises 119 automated tests across 14 modules.
# Any significant drop in test discovery or mass-skipping must immediately fail CI.
MIN_EXPECTED_TESTS = 118

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
    # `testsRun` counts discovered tests INCLUDING ones that skipped, so a run
    # where most artifact-dependent tests silently skipped would still look
    # healthy. We therefore guard on the number actually EXECUTED.
    executed = result.testsRun - len(result.skipped)
    print("\n" + "=" * 80)
    print("Test Run Summary:")
    print(f"  - Tests Discovered: {result.testsRun}")
    print(f"  - Tests Executed:   {executed}")
    print(f"  - Skipped:          {len(result.skipped)}")
    print(f"  - Failures:         {len(result.failures)}")
    print(f"  - Errors:           {len(result.errors)}")
    print(f"  - Time Elapsed:     {elapsed:.2f} seconds")
    print("=" * 80)
    if result.skipped:
        print(f"[NOTE] {len(result.skipped)} test(s) skipped (usually because generated "
              f"artifacts are absent). Run `python run_all.py` to materialize them.")

    # Guard against discovery regressions AND against mass-skipping: require the
    # historical baseline (118) of tests to actually EXECUTE, not merely be found.
    if executed < MIN_EXPECTED_TESTS:
        print(f"[FAIL] Only {executed} tests executed (discovered {result.testsRun}, "
              f"skipped {len(result.skipped)}); expected at least {MIN_EXPECTED_TESTS} "
              f"executed. Discovery may have dropped a module, or required artifacts "
              f"are missing -- run `python run_all.py` first.")
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
