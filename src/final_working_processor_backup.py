import sys
import os
import random
from datetime import datetime, timedelta
import time

# Force the Python path with absolute imports
sys.path.insert(0, '/app/src')

try:
    from database import DatabaseClient
    from minio_client import MinioClient
    from data_generator import DataGenerator
    print(" All imports successful")
except ImportError as e:
    print(f" Import error: {e}")
    print(" Current Python path:", sys.path)
    print(" Files in /app/src:", os.listdir('/app/src') if os.path.exists('/app/src') else "Directory not found")
    print(" Files in current directory:", os.listdir('.'))
    raise

class FinalBatchProcessor:
    def __init__(self):
        print(" Initializing FinalBatchProcessor...")
        try:
            self.db = DatabaseClient()
            print(" DatabaseClient initialized")
            self.minio_client = MinioClient()
            print(" MinioClient initialized")
            self.data_gen = DataGenerator()
            print(" DataGenerator initialized")
        except Exception as e:
            print(f" Initialization error: {e}")
            raise
        
    def process_large_dataset(self, total_records=1000000, chunk_size=100000, demo_limit=300000):
        print(" FINAL WORKING PROCESSOR STARTING")
        
        total_stored = 0
        chunk_num = 0
        
        for chunk_start in range(0, total_records, chunk_size):
            chunk_num += 1
            
            # Check demo limit
            if total_stored >= demo_limit:
                print(f"Demo limit reached ({demo_limit:,} records)")
                break
                
            # Generate chunk data
            print(f"Processing Chunk {chunk_num}: {chunk_size:,} records")
            chunk_data = self.data_gen.generate_batch(chunk_size)
            print(f"DEBUG: Generated {len(chunk_data)} records")
            
            # Process in smaller batches for database efficiency
            batch_size = 10000
            chunk_stored = 0
            
            for i in range(0, len(chunk_data), batch_size):
                batch_data = chunk_data[i:i + batch_size]
                
                # DEBUG: Check batch before insert
                print(f"DEBUG: Batch {i//batch_size + 1} - Records to insert: {len(batch_data)}")
                if batch_data:
                    print(f"DEBUG: First record keys: {list(batch_data[0].keys())}")
                
                # Store in database
                stored_count = self.db.insert_transaction_batch(batch_data)
                
                # DEBUG: Check what was returned
                print(f"DEBUG: Batch {i//batch_size + 1} - insert_transaction_batch returned: {stored_count}")
                
                chunk_stored += stored_count
                
                # DEBUG: Verify with actual count
                actual_count = self.db.get_transaction_count()
                print(f"DEBUG: Actual database count: {actual_count}")
                
                # Small delay to prevent overwhelming the database
                time.sleep(0.1)
            
            # Calculate success rate
            success_rate = (chunk_stored / chunk_size) * 100
            total_stored += chunk_stored
            
            print(f"  Stored: {chunk_stored:,} records")
            print(f"  Success rate: {success_rate:.1f}%")
            print(f"  Total: {total_stored:,} stored")
            print()
            
            # Store raw data in MinIO every few chunks
            if chunk_num % 3 == 0:
                self._store_raw_data_in_minio(chunk_data, chunk_num)
        
        # Final verification
        final_count = self.db.get_transaction_count()
        print("=" * 50)
        print(f"FINAL VERIFICATION:")
        print(f"Script reports: {total_stored:,} records stored")
        print(f"Database actual: {final_count:,} records in table")
        print("=" * 50)
        
        return total_stored
    
    def _store_raw_data_in_minio(self, chunk_data, chunk_num):
        """Store raw data in MinIO for backup"""
        try:
            import json
            from io import BytesIO
            
            # Convert to JSON
            json_data = json.dumps(chunk_data, indent=2)
            data_bytes = BytesIO(json_data.encode('utf-8'))
            
            # Upload to MinIO
            self.minio_client.upload_data(
                data_bytes,
                f"raw-batch-chunk-{chunk_num}.json",
                "raw-data",
                length=len(json_data)
            )
            print(f"   Raw data stored in MinIO: chunk-{chunk_num}.json")
            
        except Exception as e:
            print(f"   MinIO storage error: {e}")

def main():
    print(" Starting main function...")
    processor = FinalBatchProcessor()
    
    try:
        # Process with demo limit
        print(" Starting data processing...")
        result = processor.process_large_dataset(
            total_records=1000000,
            chunk_size=100000, 
            demo_limit=300000
        )
        
        print(" COMPLETED!")
        print(f"Final: {result:,} records stored")
        
    except Exception as e:
        print(f" ERROR in main: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        # Close database connection
        print(" Closing database connection...")
        processor.db.close()
        print(" Database connection closed")

if __name__ == "__main__":
    print(" Script started")
    main()
    print(" Script finished")