#!/usr/bin/env python3
import sys
import traceback

print("Starting test...")

try:
    print("Importing preprocess_data...")
    import preprocess_data
    print("✅ Import successful")
    
    print("Calling main function...")
    preprocess_data.main()
    print("✅ Main function completed")
    
except Exception as e:
    print(f"❌ Error: {e}")
    traceback.print_exc()
    sys.exit(1)

print("Test completed successfully")

