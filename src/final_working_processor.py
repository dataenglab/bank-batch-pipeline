import sys
import os
import time

sys.path.insert(0, '/app/src')

try:
    from database import DatabaseClient
    from data_generator import DataGenerator
    print(" Database and DataGenerator imports successful")
except ImportError as e:
    print(f" Import error: {e}")
    raise

print(" Running without MinIO storage")

class FinalBatchProcessor:
    def __init__(self):
        self.db = DatabaseClient()
        self.data_gen = DataGenerator()
        print(" Processor initialized")
        
    def process_large_dataset(self, total_records=1000000, chunk_size=100000, demo_limit=300000):
        print(" FINAL WORKING PROCESSOR STARTING")
        
        total_stored = 0
        chunk_num = 0
        
        for chunk_start in range(0, total_records, chunk_size):
            chunk_num += 1
            
            if total_stored >= demo_limit:
                print(f"Demo limit reached ({demo_limit:,} records)")
                break
                
            print(f"Processing Chunk {chunk_num}: {chunk_size:,} records")
            chunk_data = self.data_gen.generate_batch(chunk_size)
            print(f"DEBUG: Generated {len(chunk_data)} records")
            
            batch_size = 10000
            chunk_stored = 0
            
            for i in range(0, len(chunk_data), batch_size):
                batch_data = chunk_data[i:i + batch_size]
                
                # DEBUG: Check batch before insert
                print(f"DEBUG: Batch {i//batch_size + 1} - Records to insert: {len(batch_data)}")
                
                # Store in database
                stored_count = self.db.insert_transaction_batch(batch_data)
                
                # DEBUG: Check what was returned
                print(f"DEBUG: Batch {i//batch_size + 1} - insert_transaction_batch returned: {stored_count}")
                
                chunk_stored += stored_count
                
                # DEBUG: Verify with actual count
                actual_count = self.db.get_transaction_count()
                print(f"DEBUG: Actual database count: {actual_count}")
                
                time.sleep(0.1)
            
            success_rate = (chunk_stored / chunk_size) * 100
            total_stored += chunk_stored
            
            print(f"  Stored: {chunk_stored:,} records")
            print(f"  Success rate: {success_rate:.1f}%")
            print(f"  Total: {total_stored:,} stored")
            print()
        
        final_count = self.db.get_transaction_count()
        print("=" * 50)
        print(f"FINAL VERIFICATION:")
        print(f"Script reports: {total_stored:,} records stored")
        print(f"Database actual: {final_count:,} records in table")
        print("=" * 50)
        
        return total_stored

def main():
    processor = FinalBatchProcessor()
    
    try:
        result = processor.process_large_dataset(
            total_records=1000000,
            chunk_size=100000, 
            demo_limit=300000
        )
        
        print(" COMPLETED!")
        print(f"Final: {result:,} records stored")
        
    except Exception as e:
        print(f" ERROR: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        processor.db.close()

if __name__ == "__main__":
    main()