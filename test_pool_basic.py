#!/usr/bin/env python3
"""
Basic test script for connection pool functionality
Run this to verify the connection pool works with real Databricks credentials
"""

import os
import time
import threading
from dotenv import load_dotenv
from src.mcp_server.connection_pool import get_pool, PooledConnection

# Load environment variables from .env file
load_dotenv()


def test_basic_pool_functionality():
    """Test basic connection pool operations"""
    print("🔧 Testing basic connection pool functionality...")
    
    # Check environment variables
    required_vars = ['DATABRICKS_HOST', 'DATABRICKS_TOKEN', 'DATABRICKS_HTTP_PATH']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"❌ Missing environment variables: {missing_vars}")
        print("Please set these variables before running the test")
        return False
    
    try:
        # Get pool instance
        pool = get_pool()
        print(f"✅ Connection pool created with max {pool.max_connections} connections")
        
        # Test single connection
        with PooledConnection(pool) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 as test_value")
            result = cursor.fetchone()
            cursor.close()
            print(f"✅ Single connection test passed: {result}")
        
        # Test multiple connections
        results = []
        def worker(worker_id):
            try:
                with PooledConnection(pool) as conn:
                    cursor = conn.cursor()
                    cursor.execute(f"SELECT {worker_id} as worker_id, current_timestamp() as ts")
                    result = cursor.fetchone()
                    cursor.close()
                    results.append((worker_id, result))
                    print(f"✅ Worker {worker_id} completed")
            except Exception as e:
                print(f"❌ Worker {worker_id} failed: {e}")
                results.append((worker_id, None))
        
        print("\n🔄 Testing concurrent connections...")
        threads = []
        for i in range(5):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        successful_results = [r for r in results if r[1] is not None]
        print(f"✅ Concurrent test: {len(successful_results)}/5 workers succeeded")
        
        # Clean up
        pool.close_all()
        print("✅ Connection pool cleaned up")
        
        return len(successful_results) == 5
        
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        return False


def test_pool_stress():
    """Test pool under stress with many requests"""
    print("\n💪 Running stress test...")
    
    try:
        pool = get_pool()
        start_time = time.time()
        completed_requests = 0
        failed_requests = 0
        
        def stress_worker(worker_id):
            nonlocal completed_requests, failed_requests
            
            for i in range(10):  # Each worker does 10 requests
                try:
                    with PooledConnection(pool) as conn:
                        cursor = conn.cursor()
                        cursor.execute("SELECT current_timestamp()")
                        cursor.fetchone()
                        cursor.close()
                        completed_requests += 1
                except Exception:
                    failed_requests += 1
        
        # Start 20 workers (200 total requests)
        threads = []
        for i in range(20):
            thread = threading.Thread(target=stress_worker, args=(i,))
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"✅ Stress test completed in {duration:.2f} seconds")
        print(f"✅ Requests completed: {completed_requests}")
        print(f"❌ Requests failed: {failed_requests}")
        print(f"✅ Requests per second: {completed_requests/duration:.1f}")
        
        pool.close_all()
        return failed_requests == 0
        
    except Exception as e:
        print(f"❌ Stress test failed: {e}")
        return False


if __name__ == "__main__":
    print("🚀 Starting connection pool tests...\n")
    
    basic_success = test_basic_pool_functionality()
    stress_success = test_pool_stress()
    
    print(f"\n📊 Test Results:")
    print(f"  Basic functionality: {'✅ PASS' if basic_success else '❌ FAIL'}")
    print(f"  Stress test: {'✅ PASS' if stress_success else '❌ FAIL'}")
    
    if basic_success and stress_success:
        print("\n🎉 All tests passed! Connection pool is ready for MCP integration.")
    else:
        print("\n⚠️  Some tests failed. Please check the implementation.") 