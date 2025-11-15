import pandas as pd
import psycopg2
import boto3
from io import StringIO
import os
from datetime import datetime

class EnhancedBatchProcessor:
    def __init__(self):
        self.setup_storage()
        
    def setup_storage(self):
        """Initialize database and object storage connections"""
        try:
            # PostgreSQL connection
            self.pg_conn = psycopg2.connect(
                host=os.getenv('POSTGRES_HOST', 'postgres'),
                database=os.getenv('POSTGRES_DB', 'bank_data'),
                user=os.getenv('POSTGRES_USER', 'admin'),
                password=os.getenv('POSTGRES_PASSWORD', 'password123'),
                port=5432
            )
            print(" Connected to PostgreSQL")
            
            # MinIO/S3 connection
            self.s3_client = boto3.client(
                's3',
                endpoint_url=f"http://{os.getenv('MINIO_ENDPOINT', 'minio:9000')}",
                aws_access_key_id=os.getenv('MINIO_ACCESS_KEY', 'admin'),
                aws_secret_access_key=os.getenv('MINIO_SECRET_KEY', 'password123'),
                config=boto3.session.Config(signature_version='s3v4'),
                verify=False
            )
            
            # Create bucket if it doesn't exist
            try:
                self.s3_client.create_bucket(Bucket='raw-data')
                print(" Created MinIO bucket: raw-data")
            except:
                print(" MinIO bucket already exists")
                
        except Exception as e:
            print(f" Storage setup error: {e}")
            raise

    def store_raw_data(self, df, chunk_number):
        """Store raw data chunk in MinIO"""
        try:
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            
            filename = f"bank_transactions_chunk_{chunk_number}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
            self.s3_client.put_object(
                Bucket='raw-data',
                Key=filename,
                Body=csv_buffer.getvalue(),
                ContentType='text/csv'
            )
            print(f" Raw data stored in MinIO: {filename}")
            return True
        except Exception as e:
            print(f" Error storing raw data: {e}")
            return False

    def store_processed_data(self, df):
        """Store processed data in PostgreSQL"""
        try:
            cursor = self.pg_conn.cursor()
            
            # Convert date columns
            df['TransactionDate'] = pd.to_datetime(df['TransactionDate'], format='%d/%m/%y').dt.date
            
            for _, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO transactions 
                    (transaction_id, customer_id, customer_dob, cust_gender, cust_location, 
                     cust_account_balance, transaction_date, transaction_time, transaction_amount)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (transaction_id) DO NOTHING
                """, (
                    row['TransactionID'], row['CustomerID'], row['CustomerDOB'], 
                    row['CustGender'], row['CustLocation'], row['CustAccountBalance'],
                    row['TransactionDate'], row['TransactionTime'], row['TransactionAmount (INR)']
                ))
            
            self.pg_conn.commit()
            cursor.close()
            print(f" Processed {len(df)} records stored in PostgreSQL")
            return True
        except Exception as e:
            self.pg_conn.rollback()
            print(f" Error storing processed data: {e}")
            return False

    def create_aggregations(self):
        """Create business aggregations for ML ready data"""
        try:
            cursor = self.pg_conn.cursor()
            
            # Daily transactions aggregation
            cursor.execute("""
                INSERT INTO daily_transactions_agg
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
                    unique_customers = EXCLUDED.unique_customers
            """)
            
            # Customer behavior aggregation
            cursor.execute("""
                INSERT INTO customer_behavior_agg
                SELECT 
                    customer_id,
                    transaction_date as aggregation_date,
                    COUNT(*) as total_transactions,
                    SUM(transaction_amount) as total_spent,
                    AVG(transaction_amount) as avg_transaction_amount
                FROM transactions
                GROUP BY customer_id, transaction_date
                ON CONFLICT (customer_id, aggregation_date) DO UPDATE SET
                    total_transactions = EXCLUDED.total_transactions,
                    total_spent = EXCLUDED.total_spent,
                    avg_transaction_amount = EXCLUDED.avg_transaction_amount
            """)
            
            self.pg_conn.commit()
            cursor.close()
            print(" Aggregations created for ML ready data")
            return True
        except Exception as e:
            self.pg_conn.rollback()
            print(f" Error creating aggregations: {e}")
            return False

    def process_full_pipeline(self):
        """Run the complete batch processing pipeline"""
        print(" ENHANCED BATCH PROCESSING PIPELINE STARTING")
        print("=" * 60)
        
        file_path = '/app/data/bank_transactions.csv'
        chunk_size = 100000
        total_processed = 0
        
        try:
            # Process data in chunks
            for chunk_number, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size), 1):
                print(f"\n Processing Chunk {chunk_number}...")
                
                # Store raw data in MinIO
                if self.store_raw_data(chunk, chunk_number):
                    print(f"   Raw data stored")
                
                # Store processed data in PostgreSQL
                if self.store_processed_data(chunk):
                    print(f"   Processed data stored")
                
                total_processed += len(chunk)
                print(f"   Chunk {chunk_number} completed: {len(chunk)} records")
                
                # Demo limit - remove this for full processing
                if total_processed >= 300000:
                    print(f"    Demo limit reached (300k records)")
                    break
            
            # Create aggregations
            print(f"\n Creating aggregations...")
            if self.create_aggregations():
                print("   Aggregations created")
            
            # Final summary
            print("\n" + "=" * 60)
            print(" PIPELINE COMPLETED SUCCESSFULLY!")
            print(f" Total records processed: {total_processed:,}")
            print(f" Raw data: MinIO object storage")
            print(f"  Processed data: PostgreSQL database")
            print(f" ML-ready data: Aggregation tables")
            print("=" * 60)
            
        except Exception as e:
            print(f" Pipeline error: {e}")
            return False
        
        return True

if __name__ == "__main__":
    processor = EnhancedBatchProcessor()
    success = processor.process_full_pipeline()
    
    if processor.pg_conn:
        processor.pg_conn.close()
    
    exit(0 if success else 1)