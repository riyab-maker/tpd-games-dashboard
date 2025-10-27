#!/usr/bin/env python3
"""
Test script to verify all processors work correctly
"""

import os
import sys
import subprocess
from datetime import datetime

def test_processor(processor_name: str) -> bool:
    """Test a single processor"""
    print(f"\n🧪 Testing {processor_name}...")
    
    try:
        result = subprocess.run(
            [sys.executable, f"{processor_name}_processor.py"],
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        if result.returncode == 0:
            print(f"✅ {processor_name} completed successfully")
            return True
        else:
            print(f"❌ {processor_name} failed with return code {result.returncode}")
            print(f"Error output: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"⏰ {processor_name} timed out after 5 minutes")
        return False
    except Exception as e:
        print(f"❌ {processor_name} failed with exception: {e}")
        return False

def main():
    """Test all processors"""
    print("🚀 Testing All Data Processors")
    print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    processors = [
        "conversion_funnel",
        "timeseries", 
        "repeatability",
        "score_distribution"
    ]
    
    results = {}
    
    for processor in processors:
        results[processor] = test_processor(processor)
    
    # Print summary
    print("\n" + "="*50)
    print("📊 TEST SUMMARY")
    print("="*50)
    
    successful = sum(results.values())
    total = len(results)
    
    print(f"✅ Successful: {successful}/{total}")
    
    for processor, success in results.items():
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"  {processor}: {status}")
    
    if successful == total:
        print("\n🎉 All processors passed!")
        print("📤 Ready for production deployment!")
    else:
        print(f"\n⚠️ {total - successful} processors failed")
        print("🔧 Please fix the failing processors before deployment")
    
    print(f"\n⏰ Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()
