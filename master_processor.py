#!/usr/bin/env python3
"""
Master Data Processor

This script orchestrates all data processing tasks:
- Runs all individual processors in sequence
- Handles errors gracefully
- Provides comprehensive logging
- Optimized for local processing before GitHub upload

Usage:
    python master_processor.py [--skip-errors] [--processors conversion,timeseries,repeatability,score]
"""

import os
import sys
import time
import argparse
from datetime import datetime
from typing import List, Dict, Any
import subprocess
import traceback

# Import individual processors
try:
    from conversion_funnel_processor import main as process_conversion
    from timeseries_processor import main as process_timeseries
    from repeatability_processor import main as process_repeatability
    from score_distribution_processor import main as process_score
except ImportError as e:
    print(f"❌ Error importing processors: {e}")
    print("Make sure all processor files are in the same directory")
    sys.exit(1)

class MasterProcessor:
    def __init__(self, skip_errors: bool = False, processors: List[str] = None):
        self.skip_errors = skip_errors
        self.processors = processors or ['conversion', 'timeseries', 'repeatability', 'score']
        self.results = {}
        self.start_time = datetime.now()
        
    def log(self, message: str, level: str = "INFO"):
        """Log message with timestamp"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] [{level}] {message}")
    
    def run_processor(self, name: str, processor_func) -> bool:
        """Run a single processor and handle errors"""
        self.log(f"🚀 Starting {name} processor...")
        start_time = time.time()
        
        try:
            processor_func()
            duration = time.time() - start_time
            self.log(f"✅ {name} processor completed successfully in {duration:.2f}s")
            self.results[name] = {'status': 'success', 'duration': duration}
            return True
            
        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"❌ {name} processor failed after {duration:.2f}s: {str(e)}"
            self.log(error_msg, "ERROR")
            self.log(f"Traceback: {traceback.format_exc()}", "DEBUG")
            
            self.results[name] = {'status': 'error', 'duration': duration, 'error': str(e)}
            
            if self.skip_errors:
                self.log(f"⚠️ Skipping {name} processor due to error (--skip-errors enabled)")
                return False
            else:
                self.log(f"🛑 Stopping processing due to error in {name}")
                return False
    
    def run_all_processors(self):
        """Run all enabled processors"""
        self.log("🎯 Master Data Processor Starting...")
        self.log(f"📋 Processors to run: {', '.join(self.processors)}")
        self.log(f"⚙️ Skip errors: {self.skip_errors}")
        
        # Define processor mappings
        processor_map = {
            'conversion': process_conversion,
            'timeseries': process_timeseries,
            'repeatability': process_repeatability,
            'score': process_score
        }
        
        successful_processors = []
        failed_processors = []
        
        for processor_name in self.processors:
            if processor_name not in processor_map:
                self.log(f"⚠️ Unknown processor: {processor_name}", "WARNING")
                continue
                
            success = self.run_processor(processor_name, processor_map[processor_name])
            
            if success:
                successful_processors.append(processor_name)
            else:
                failed_processors.append(processor_name)
                if not self.skip_errors:
                    break
        
        # Print summary
        self.print_summary(successful_processors, failed_processors)
        
        return len(failed_processors) == 0
    
    def print_summary(self, successful: List[str], failed: List[str]):
        """Print processing summary"""
        total_duration = (datetime.now() - self.start_time).total_seconds()
        
        print("\n" + "="*60)
        print("📊 PROCESSING SUMMARY")
        print("="*60)
        
        print(f"⏰ Total Duration: {total_duration:.2f}s")
        print(f"✅ Successful: {len(successful)} processors")
        print(f"❌ Failed: {len(failed)} processors")
        
        if successful:
            print(f"\n✅ Successful Processors:")
            for name in successful:
                duration = self.results[name]['duration']
                print(f"  • {name}: {duration:.2f}s")
        
        if failed:
            print(f"\n❌ Failed Processors:")
            for name in failed:
                duration = self.results[name]['duration']
                error = self.results[name].get('error', 'Unknown error')
                print(f"  • {name}: {duration:.2f}s - {error}")
        
        print("\n📁 Generated Files:")
        self.list_generated_files()
        
        print("\n" + "="*60)
        
        if failed:
            print("⚠️ Some processors failed. Check logs above for details.")
            if not self.skip_errors:
                print("💡 Use --skip-errors to continue processing other components.")
        else:
            print("🎉 All processors completed successfully!")
            print("📤 Ready for GitHub upload!")
    
    def list_generated_files(self):
        """List all generated files in processed_data/"""
        processed_dir = 'processed_data'
        if os.path.exists(processed_dir):
            files = os.listdir(processed_dir)
            csv_files = [f for f in files if f.endswith('.csv')]
            
            if csv_files:
                for file in sorted(csv_files):
                    file_path = os.path.join(processed_dir, file)
                    size = os.path.getsize(file_path)
                    print(f"  • {file} ({size:,} bytes)")
            else:
                print("  • No CSV files found")
        else:
            print("  • processed_data/ directory not found")

def main():
    """Main function with command line argument parsing"""
    parser = argparse.ArgumentParser(description='Master Data Processor for Hybrid Dashboard')
    parser.add_argument('--skip-errors', action='store_true', 
                       help='Continue processing even if some processors fail')
    parser.add_argument('--processors', type=str, 
                       help='Comma-separated list of processors to run (conversion,timeseries,repeatability,score)')
    
    args = parser.parse_args()
    
    # Parse processors list
    processors = None
    if args.processors:
        processors = [p.strip() for p in args.processors.split(',')]
        valid_processors = ['conversion', 'timeseries', 'repeatability', 'score']
        invalid = [p for p in processors if p not in valid_processors]
        if invalid:
            print(f"❌ Invalid processors: {invalid}")
            print(f"Valid processors: {', '.join(valid_processors)}")
            sys.exit(1)
    
    # Create and run master processor
    master = MasterProcessor(skip_errors=args.skip_errors, processors=processors)
    success = master.run_all_processors()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
