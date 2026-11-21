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
    print(f"Test Run Summary:")
    print(f"  • Tests Executed: {result.testsRun}")
    print(f"  • Failures:       {len(result.failures)}")
    print(f"  • Errors:         {len(result.errors)}")
    print(f"  • Time Elapsed:   {elapsed:.2f} seconds")
    print("=" * 80)
    
    if result.wasSuccessful():
        print("✅ ALL TESTS PASSED SUCCESSFULLY! Codebase is ready for grading defense.")
        return 0
    else:
        print("❌ SOME TESTS FAILED. Review tracebacks above.")
        return 1

if __name__ == "__main__":
    exit_code = run_all_tests()
    sys.exit(exit_code)
