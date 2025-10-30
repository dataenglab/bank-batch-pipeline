#!/usr/bin/env python3
"""
Final Fixed Enhanced Batch Processor with correct date handling and connection setup
"""

import pandas as pd
import psycopg2
from io import BytesIO
import os
from datetime import datetime

# Try to import minio
try:
    from minio import Minio
    MINIO_AVAILABLE = True
except ImportError:
    print("MinIO package not available - raw data storage disabled")
    MINIO_AVAILABLE = False

# Try to import the correct config based on environment
try:
    # First try local config (for testing outside Docker)
    from config_local import DB_CONFIG, MINIO_CONFIG
    print("Using local config for testing")
except ImportError:
    try:
        # Fall back to Docker config
        from config import DB_CONFIG, MINIO_CONFIG
        print("Using Docker config")
    except ImportError:
        # Final fallback with correct credentials from docker-compose
        DB_CONFIG = {
            'host': 'postgres',
            'port': 5432,
            'database': 'bank_data',
            'user': 'admin', 
            'password': 'password123'
        }
        MINIO_CONFIG = {
            'endpoint': 'minio:9000',
            'access_key': 'admin',
            'secret_key': 'password123',
            'secure': False
        }
        print("Using fallback config")

class FinalFixedEnhancedBatchProcessor:
    def __init__(self):
        self.setup_storage()
        
    def setup_storage(self):
        print("Setting up storage connections...")
        
        try:
            # PostgreSQL connection
            print(f"Connecting to PostgreSQL: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
            self.pg_conn = psycopg2.connect(**DB_CONFIG)
            self.pg_conn.autocommit = False
            
            # MinIO client
            if MINIO_AVAILABLE:
                self.minio_client = Minio(
                    MINIO_CONFIG['endpoint'],
                    access_key=MINIO_CONFIG['access_key'],
                    secret_key=MINIO_CONFIG['secret_key'],
                    secure=MINIO_CONFIG.get('secure', False)
                )
                
                if not self.minio_client.bucket_exists("raw-data"):
                    self.minio_client.make_bucket("raw-data")
                    print("Created MinIO bucket: raw-data")
                else:
                    print("MinIO bucket already exists")
            else:
                self.minio_client = None
                print("MinIO storage disabled")
                
        except Exception as e:
            print(f"Storage setup error: {e}")
            raise

    def smart_date_conversion(self, date_str, field_name=""):
        """
        Smart date conversion that handles both M/D/YY and D/M/YY formats
        Based on logical constraints for each field type
        """
        if pd.isna(date_str) or date_str == "" or date_str is None:
            return None
            
        date_str = str(date_str).strip()
        
        # Try different parsing strategies based on field type
        if field_name == "TransactionDate":
            # Transaction dates are likely recent (2000-2020)
            # Prefer M/D/YY format for transaction dates
            formats = ['%m/%d/%y', '%d/%m/%y', '%Y-%m-%d', '%m-%d-%Y']
        elif field_name == "CustomerDOB":
            # Customer DOBs are likely older (1950-2000) 
            # Prefer D/M/YY format for DOB (more common internationally)
            formats = ['%d/%m/%y', '%m/%d/%y', '%Y-%m-%d', '%d-%m-%Y']
        else:
            # Default: try both formats
            formats = ['%m/%d/%y', '%d/%m/%y', '%Y-%m-%d']
        
        for fmt in formats:
            try:
                parsed_date = pd.to_datetime(date_str, format=fmt)
                
                # Validate based on field type
                if field_name == "TransactionDate":
                    # Transaction dates should be reasonable (2000-2020)
                    if 2000 <= parsed_date.year <= 2020:
                        return parsed_date.date()
                elif field_name == "CustomerDOB":
                    # Customer DOBs should be reasonable (1930-2010)
                    if 1930 <= parsed_date.year <= 2010:
                        return parsed_date.date()
                else:
                    # For other fields, accept any valid date
                    return parsed_date.date()
                    
            except:
                continue
        
        # Final fallback: pandas automatic parsing
        try:
            parsed_date = pd.to_datetime(date_str)
            return parsed_date.date()
        except:
            return None

    def robust_numeric_conversion(self, value):
        """Safe numeric conversion"""
        if pd.isna(value) or value == "" or value is None:
            return None
            
        try:
            if isinstance(value, str):
                value = value.replace(',', '').replace(' ', '').strip()
            return float(value)
        except (ValueError, TypeError):
            return None

    def store_raw_data_minio(self, df, filename):
        if not MINIO_AVAILABLE or self.minio_client is None:
            print(f"MinIO not available - skipping raw data storage: {filename}")
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
            print(f"Raw data stored in MinIO: {filename}")
            return True
        except Exception as e:
            print(f"Error storing raw data: {e}")
            return False

    def store_processed_data_postgres(self, df):
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
                    # Convert dates with SMART format detection
                    transaction_date = self.smart_date_conversion(row['TransactionDate'], "TransactionDate")
                    customer_dob = self.smart_date_conversion(row['CustomerDOB'], "CustomerDOB")
                    
                    # Skip rows with critical missing transaction date
                    if transaction_date is None:
                        date_errors += 1
                        continue
                        
                    # Handle numeric conversions
                    cust_balance = self.robust_numeric_conversion(row['CustAccountBalance'])
                    transaction_amount = self.robust_numeric_conversion(row['TransactionAmount (INR)'])
                    
                    # Skip rows with invalid numeric values
                    if cust_balance is None or transaction_amount is None:
                        numeric_errors += 1
                        continue
                    
                    # Prepare other fields with defaults
                    transaction_id = str(row['TransactionID']) if pd.notna(row['TransactionID']) else None
                    customer_id = str(row['CustomerID']) if pd.notna(row['CustomerID']) else None
                    cust_gender = str(row['CustGender'])[:1] if pd.notna(row['CustGender']) else 'U'
                    cust_location = str(row['CustLocation']) if pd.notna(row['CustLocation']) else 'Unknown'
                    transaction_time = str(row['TransactionTime']) if pd.notna(row['TransactionTime']) else '000000'
                    
                    # Provide default for optional CustomerDOB
                    if customer_dob is None:
                        customer_dob = datetime(1900, 1, 1).date()
                    
                    # Skip if critical IDs are missing
                    if not transaction_id or not customer_id:
                        other_errors += 1
                        continue

                    # Debug: Print first few successful conversions
                    if records_stored < 3:
                        print(f"  DEBUG: Successfully converted - TransactionDate: {transaction_date}, CustomerDOB: {customer_dob}")

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
                print(f"Processed {records_stored} records stored in PostgreSQL")
            else:
                self.pg_conn.rollback()
                print("No records stored - transaction rolled back")

            if problematic_rows > 0:
                print(f"Skipped {problematic_rows} problematic rows")
                print(f"  - Date conversion errors: {date_errors}")
                print(f"  - Numeric conversion errors: {numeric_errors}")
                print(f"  - Other errors: {other_errors}")

            return records_stored

        except Exception as e:
            if cursor:
                self.pg_conn.rollback()
            print(f"Error storing processed data: {e}")
            return 0
        finally:
            if cursor:
                cursor.close()

    def create_aggregations(self):
        """Create business aggregations for ML ready data"""
        try:
            cursor = self.pg_conn.cursor()

            # Check if aggregation tables exist, create if not
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS daily_transactions_aggregation (
                    aggregation_date DATE PRIMARY KEY,
                    total_transactions INTEGER,
                    total_amount DECIMAL(15,2),
                    avg_transaction_amount DECIMAL(10,2),
                    unique_customers INTEGER
                );
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS customer_behavior_aggregation (
                    customer_id VARCHAR(50) PRIMARY KEY,
                    total_transactions INTEGER,
                    total_amount DECIMAL(15,2),
                    avg_transaction_amount DECIMAL(10,2),
                    first_transaction_date DATE,
                    last_transaction_date DATE,
                    preferred_location TEXT
                );
            """)

            # Daily transactions aggregation
            cursor.execute("""
                INSERT INTO daily_transactions_aggregation
                SELECT
                    transaction_date as aggregation_date,
                    COUNT(*) as total_transactions,
                    SUM(transaction_amount) as total_amount,
                    AVG(transaction_amount) as avg_transaction_amount,
                    COUNT(DISTINCT customer_id) as unique_customers
                FROM transactions
                GROUP BY transaction_date
                ON CONFLICT (aggregation_date) DO UPDATE SET
                    total_transactions = EXCLUDED.total_transactions,
                    total_amount = EXCLUDED.total_amount,
                    avg_transaction_amount = EXCLUDED.avg_transaction_amount,
                    unique_customers = EXCLUDED.unique_customers;
            """)

            # Customer behavior aggregation
            cursor.execute("""
                INSERT INTO customer_behavior_aggregation
                SELECT
                    customer_id,
                    COUNT(*) as total_transactions,
                    SUM(transaction_amount) as total_amount,
                    AVG(transaction_amount) as avg_transaction_amount,
                    MIN(transaction_date) as first_transaction_date,
                    MAX(transaction_date) as last_transaction_date,
                    MODE() WITHIN GROUP (ORDER BY cust_location) as preferred_location
                FROM transactions
                GROUP BY customer_id
                ON CONFLICT (customer_id) DO UPDATE SET
                    total_transactions = EXCLUDED.total_transactions,
                    total_amount = EXCLUDED.total_amount,
                    avg_transaction_amount = EXCLUDED.avg_transaction_amount,
                    first_transaction_date = EXCLUDED.first_transaction_date,
                    last_transaction_date = EXCLUDED.last_transaction_date,
                    preferred_location = EXCLUDED.preferred_location;
            """)

            self.pg_conn.commit()
            cursor.close()
            print("Aggregations created successfully")
            return True
        except Exception as e:
            self.pg_conn.rollback()
            print(f"Error creating aggregations: {e}")
            return False

    def process_chunk_enhanced(self, chunk, chunk_number):
        print(f"Processing Chunk {chunk_number}...")
        print(f"  Records in chunk: {len(chunk):,}")
        
        try:
            # Store raw data
            raw_filename = f"bank_transactions_chunk_{chunk_number}.csv"
            self.store_raw_data_minio(chunk, raw_filename)
            
            # Process and store in PostgreSQL
            success_count = self.store_processed_data_postgres(chunk)
            
            print(f"  Chunk {chunk_number} completed: {success_count:,} records stored")
            return success_count
            
        except Exception as e:
            print(f"Chunk {chunk_number} failed: {e}")
            return 0

    def test_connections(self):
        print("Testing connections...")
        try:
            # Test PostgreSQL
            cursor = self.pg_conn.cursor()
            cursor.execute("SELECT version();")
            pg_version = cursor.fetchone()[0]
            print(f"PostgreSQL: {pg_version.split(',')[0]}")
            
            # Check if transactions table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'transactions'
                );
            """)
            table_exists = cursor.fetchone()[0]
            print(f"Transactions table exists: {table_exists}")
            cursor.close()
            
            # Test MinIO
            if MINIO_AVAILABLE and self.minio_client:
                buckets = self.minio_client.list_buckets()
                print("MinIO: Connection successful")
            else:
                print("MinIO: Not available")
            return True
            
        except Exception as e:
            print(f"Connection test failed: {e}")
            return False

    def process_enhanced_pipeline(self):
        print("FINAL FIXED ENHANCED BATCH PROCESSING PIPELINE STARTING")
        print("=" * 60)

        if not self.test_connections():
            print("Storage connections failed - exiting")
            return False

        file_path = '../data/bank_transactions.csv'
        chunk_size = 100000
        total_processed = 0
        demo_limit = 300000
        total_stored = 0

        try:
            print(f"Processing file: {file_path}")

            for chunk_number, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size), 1):
                if total_processed >= demo_limit:
                    print(f"Demo limit reached ({demo_limit:,} records)")
                    break
                    
                success_count = self.process_chunk_enhanced(chunk, chunk_number)
                total_processed += len(chunk)
                total_stored += success_count
                
                if success_count == 0:
                    print(f"WARNING: No records stored from chunk {chunk_number}")

            print(f"\nTotal records processed: {total_processed:,}")
            print(f"Total records stored in database: {total_stored:,}")
            
            if total_stored > 0:
                print("\nCreating aggregations...")
                self.create_aggregations()
            else:
                print("Skipping aggregations - no data stored")

            print("FINAL FIXED PIPELINE COMPLETED!")
            return True

        except Exception as e:
            print(f"Pipeline error: {e}")
            return False
        finally:
            if hasattr(self, 'pg_conn') and self.pg_conn:
                self.pg_conn.close()

if __name__ == "__main__":
    print("Initializing Final Fixed Enhanced Batch Processor...")
    processor = FinalFixedEnhancedBatchProcessor()
    success = processor.process_enhanced_pipeline()
    exit(0 if success else 1)