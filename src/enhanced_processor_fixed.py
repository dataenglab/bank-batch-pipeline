#!/usr/bin/env python3
"""
Fixed Enhanced Batch Processor with better error handling and data validation
"""

import pandas as pd
import psycopg2
from io import StringIO, BytesIO
import os
from datetime import datetime

# Try to import minio, but provide fallback
try:
    from minio import Minio
    MINIO_AVAILABLE = True
except ImportError:
    print("  MinIO package not available - raw data storage disabled")
    MINIO_AVAILABLE = False

# Import config with fallback
try:
    from config import DB_CONFIG, MINIO_CONFIG
except ImportError:
    # Fallback config - adjust based on your actual config
    DB_CONFIG = {
        'host': 'localhost',
        'port': 5432,
        'database': 'bank_transactions',
        'user': 'postgres',
        'password': 'postgres'
    }
    MINIO_CONFIG = {
        'endpoint': 'localhost:9000',
        'access_key': 'minioadmin',
        'secret_key': 'minioadmin'
    }

class FixedEnhancedBatchProcessor:
    def __init__(self):
        self.setup_storage()
        
    def setup_storage(self):
        """Setup storage connections with better error handling"""
        print(" Setting up storage connections...")
        
        try:
            # PostgreSQL connection
            self.pg_conn = psycopg2.connect(**DB_CONFIG)
            self.pg_conn.autocommit = False
            
            # MinIO client (only if available)
            if MINIO_AVAILABLE:
                self.minio_client = Minio(
                    MINIO_CONFIG['endpoint'],
                    access_key=MINIO_CONFIG['access_key'],
                    secret_key=MINIO_CONFIG['secret_key'],
                    secure=False
                )
                
                # Create bucket if not exists
                if not self.minio_client.bucket_exists("raw-data"):
                    self.minio_client.make_bucket("raw-data")
                    print(" Created MinIO bucket: raw-data")
                else:
                    print(" MinIO bucket already exists")
            else:
                self.minio_client = None
                print("  MinIO storage disabled")
                
        except Exception as e:
            print(f" Storage setup error: {e}")
            raise

    def robust_date_conversion(self, date_str):
        """More robust date conversion with multiple format support"""
        if pd.isna(date_str) or date_str == "" or date_str is None:
            return None
            
        date_str = str(date_str).strip()
        
        # Try multiple date formats
        formats = [
            '%d/%m/%y',    # 15/10/16
            '%d/%m/%Y',    # 15/10/2016
            '%d-%m-%Y',    # 15-10-2016
            '%Y-%m-%d',    # 2016-10-15
            '%m/%d/%Y',    # 10/15/2016 (US format)
        ]
        
        for fmt in formats:
            try:
                return pd.to_datetime(date_str, format=fmt).date()
            except:
                continue
                
        # If all formats fail, try pandas automatic parsing
        try:
            return pd.to_datetime(date_str).date()
        except:
            return None

    def robust_numeric_conversion(self, value):
        """Safe numeric conversion"""
        if pd.isna(value) or value == "" or value is None:
            return None
            
        try:
            # Handle string with commas, spaces, etc.
            if isinstance(value, str):
                value = value.replace(',', '').replace(' ', '').strip()
            return float(value)
        except (ValueError, TypeError):
            return None

    def store_raw_data_minio(self, df, filename):
        """Store raw data in MinIO"""
        if not MINIO_AVAILABLE or self.minio_client is None:
            print(f"    MinIO not available - skipping raw data storage: {filename}")
            return True
            
        try:
            csv_data = df.to_csv(index=False).encode('utf-8')
            self.minio_client.put_object(
                "raw-data",
                filename,
                BytesIO(csv_data),
                length=len(csv_data),
                content_type='text/csv'
            )
            print(f"   Raw data stored in MinIO: {filename}")
            return True
        except Exception as e:
            print(f"   Error storing raw data: {e}")
            return False

    def store_processed_data_postgres(self, df):
        """Store processed data in PostgreSQL with transaction safety"""
        cursor = None
        records_stored = 0
        problematic_rows = 0
        date_errors = 0
        numeric_errors = 0
        other_errors = 0

        try:
            cursor = self.pg_conn.cursor()

            for index, row in df.iterrows():
                try:
                    # Convert dates with robust method
                    transaction_date = self.robust_date_conversion(row['TransactionDate'])
                    customer_dob = self.robust_date_conversion(row['CustomerDOB'])
                    
                    # Skip rows with critical missing dates
                    if transaction_date is None:
                        date_errors += 1
                        continue
                        
                    # Handle numeric conversions
                    cust_balance = self.robust_numeric_conversion(row['CustAccountBalance'])
                    
                    # Try different column names for transaction amount
                    transaction_amount = None
                    for col_name in ['TransactionAmount (INR)', 'TransactionAmount', 'transaction_amount']:
                        if col_name in row:
                            transaction_amount = self.robust_numeric_conversion(row[col_name])
                            if transaction_amount is not None:
                                break
                    
                    # Skip rows with invalid numeric values
                    if cust_balance is None or transaction_amount is None:
                        numeric_errors += 1
                        continue
                    
                    # Prepare other fields with defaults
                    transaction_id = str(row['TransactionID']) if pd.notna(row.get('TransactionID')) else None
                    customer_id = str(row['CustomerID']) if pd.notna(row.get('CustomerID')) else None
                    cust_gender = str(row['CustGender'])[:1] if pd.notna(row.get('CustGender')) else 'U'
                    cust_location = str(row['CustLocation']) if pd.notna(row.get('CustLocation')) else 'Unknown'
                    transaction_time = str(row['TransactionTime']) if pd.notna(row.get('TransactionTime')) else '000000'
                    
                    # Skip if critical IDs are missing
                    if not transaction_id or not customer_id:
                        other_errors += 1
                        continue

                    # Insert with individual transaction safety
                    cursor.execute("""
                        INSERT INTO transactions
                        (transaction_id, customer_id, customer_dob, cust_gender, cust_location,
                         cust_account_balance, transaction_date, transaction_time, transaction_amount)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (transaction_id) DO NOTHING
                    """, (
                        transaction_id,
                        customer_id,
                        customer_dob,
                        cust_gender,
                        cust_location,
                        cust_balance,
                        transaction_date,
                        transaction_time,
                        transaction_amount
                    ))
                    
                    records_stored += 1

                except Exception as row_error:
                    problematic_rows += 1
                    other_errors += 1
                    continue

            # Commit only if we have successful inserts
            if records_stored > 0:
                self.pg_conn.commit()
            else:
                self.pg_conn.rollback()

            print(f"   Processed {records_stored} records stored in PostgreSQL")
            if problematic_rows > 0:
                print(f"    Skipped {problematic_rows} problematic rows")
                print(f"     - Date conversion errors: {date_errors}")
                print(f"     - Numeric conversion errors: {numeric_errors}")
                print(f"     - Other errors: {other_errors}")

            return records_stored

        except Exception as e:
            if cursor:
                self.pg_conn.rollback()
            print(f"   Error storing processed data: {e}")
            return 0
        finally:
            if cursor:
                cursor.close()

    def process_chunk_enhanced(self, chunk, chunk_number):
        """Enhanced chunk processing with better data validation"""
        print(f" Processing Chunk {chunk_number}...")
        print(f"   Records in chunk: {len(chunk):,}")
        
        try:
            # Step 1: Store raw data in MinIO
            raw_filename = f"bank_transactions_chunk_{chunk_number}.csv"
            self.store_raw_data_minio(chunk, raw_filename)
            
            # Step 2: Process and store in PostgreSQL
            success_count = self.store_processed_data_postgres(chunk)
            
            print(f"   Chunk {chunk_number} completed: {success_count:,} records stored")
            return success_count
            
        except Exception as e:
            print(f" Chunk {chunk_number} failed: {e}")
            return 0

    def test_connections(self):
        """Test storage connections"""
        print(" Testing connections...")
        try:
            # Test PostgreSQL
            cursor = self.pg_conn.cursor()
            cursor.execute("SELECT version();")
            pg_version = cursor.fetchone()[0]
            print(f" PostgreSQL: {pg_version.split(',')[0]}")
            cursor.close()
            
            # Test MinIO
            if MINIO_AVAILABLE and self.minio_client:
                buckets = self.minio_client.list_buckets()
                print(" MinIO: Connection successful")
            else:
                print("  MinIO: Not available")
            return True
            
        except Exception as e:
            print(f" Connection test failed: {e}")
            return False

    def process_enhanced_pipeline(self):
        """Run the complete enhanced batch processing pipeline"""
        print(" FIXED ENHANCED BATCH PROCESSING PIPELINE STARTING")
        print("=" * 60)

        # Test connections first
        if not self.test_connections():
            print(" Storage connections failed - exiting")
            return False

        file_path = '/app/data/bank_transactions.csv'
        chunk_size = 100000
        total_processed = 0
        demo_limit = 300000

        try:
            print(f"\n Processing file: {file_path}")

            # Process data in chunks
            for chunk_number, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size), 1):
                if total_processed >= demo_limit:
                    print(f"  ⏸  Demo limit reached ({demo_limit:,} records)")
                    break
                    
                success_count = self.process_chunk_enhanced(chunk, chunk_number)
                total_processed += len(chunk)
                
                if success_count == 0:
                    print(f"    No records stored from chunk {chunk_number} - possible data quality issues")

            print(f"\n Total records processed: {total_processed:,}")
            print(" FIXED PIPELINE COMPLETED!")
            return True

        except Exception as e:
            print(f" Pipeline error: {e}")
            return False
        finally:
            if hasattr(self, 'pg_conn') and self.pg_conn:
                self.pg_conn.close()

if __name__ == "__main__":
    print("Initializing Fixed Enhanced Batch Processor...")
    processor = FixedEnhancedBatchProcessor()
    success = processor.process_enhanced_pipeline()
    exit(0 if success else 1)