#!/usr/bin/env python3
"""
Setup script for Render deployment

This script ensures all required data files are present before deployment.
Run this locally before pushing to GitHub for Render deployment.
"""

import os
import sys
from pathlib import Path

def check_environment():
    """Check if .env file exists and has required variables"""
    env_file = Path('.env')
    if not env_file.exists():
        print("ERROR: .env file not found!")
        print("Please create .env file from env.template:")
        print("  cp env.template .env")
        print("  # Edit .env with your database credentials")
        return False
    
    # Check if required environment variables are set
    from dotenv import load_dotenv
    load_dotenv()
    
    required_vars = ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"ERROR: Missing environment variables: {', '.join(missing_vars)}")
        print("Please update your .env file with the required database credentials.")
        return False
    
    print("SUCCESS: Environment variables configured")
    return True

def check_data_files():
    """Check if data files exist"""
    data_dir = Path('data')
    required_files = [
        'processed_data.csv',
        'summary_data.csv',
        'score_distribution_data.csv',
        'time_series_data.csv',
        'repeatability_data.csv',
        'metadata.json'
    ]
    
    if not data_dir.exists():
        print("ERROR: data/ directory not found!")
        return False
    
    missing_files = []
    for file in required_files:
        if not (data_dir / file).exists():
            missing_files.append(file)
    
    if missing_files:
        print(f"ERROR: Missing data files: {', '.join(missing_files)}")
        return False
    
    print("SUCCESS: All data files present")
    return True

def generate_data():
    """Generate data files using preprocessing script"""
    print("Generating data files...")
    try:
        import subprocess
        result = subprocess.run([sys.executable, 'preprocess_data.py'], 
                              capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"ERROR: Preprocessing failed!")
            print(f"Error: {result.stderr}")
            return False
        
        print("SUCCESS: Data files generated successfully")
        return True
    except Exception as e:
        print(f"ERROR: Failed to run preprocessing: {e}")
        return False

def main():
    """Main setup function"""
    print("Setting up deployment for Render...")
    print("=" * 50)
    
    # Check environment
    if not check_environment():
        return False
    
    # Check if data files exist
    if not check_data_files():
        print("\nGenerating data files...")
        if not generate_data():
            return False
    
    print("\nSUCCESS: Setup complete! Ready for deployment.")
    print("\nNext steps:")
    print("1. Commit and push changes to GitHub")
    print("2. Deploy to Render")
    print("3. The dashboard will run efficiently on Render's free tier")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
