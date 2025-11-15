# main.py
# ... imports ...

import argparse
import sys

import pandas as pd

from simple_batch_backup import EnhancedBatchProcessor
from test_data_fix import setup_connections


def main():
    """Main application entry point"""
    parser = argparse.ArgumentParser(description='Bank Batch Data Processor')
    parser.add_argument('--input', '-i', required=True, help='Input CSV file path')
    parser.add_argument('--test', '-t', action='store_true', help='Run connection tests')
    
    args = parser.parse_args()
    
    print("=== Starting Bank Batch Pipeline ===")
    
    try:
        # Setup connections
        pg_conn, s3_client = setup_connections()
        
        # Initialize processor
        processor = EnhancedBatchProcessor(pg_conn, s3_client)
        
        if args.test:
            # Test connections only
            processor.test_connections()
            processor.setup_storage()
            print(" All tests passed!")
            return
        
        # Load and process data
        print(f"Loading data from {args.input}...")
        data = pd.read_csv(args.input)
        print(f"Loaded {len(data)} records")
        
        # Process data
        print("Processing data...")
        results = processor.process_batch(data)
        
        # Generate report
        report = processor.generate_report(results)
        print(report)
        
        processor.close_connections()
        print(" Processing completed successfully!")
        
    except Exception as e:
        print(f"ERROR: Processing failed: {e}")
        sys.exit(1)