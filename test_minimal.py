#!/usr/bin/env python3
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

print("Environment variables loaded:")
print(f"DB_HOST: {os.getenv('DB_HOST')}")
print(f"DB_NAME: {os.getenv('DB_NAME')}")
print(f"DB_USER: {os.getenv('DB_USER')}")
print(f"DB_PASSWORD: {'*' * len(os.getenv('DB_PASSWORD', '')) if os.getenv('DB_PASSWORD') else 'None'}")

# Test if the script can be imported
try:
    print("\nTesting import of preprocess_data...")
    import preprocess_data
    print("✅ preprocess_data imported successfully")
    
    # Test if main function exists
    if hasattr(preprocess_data, 'main'):
        print("✅ main function found")
    else:
        print("❌ main function not found")
        
except Exception as e:
    print(f"❌ Error importing preprocess_data: {e}")

