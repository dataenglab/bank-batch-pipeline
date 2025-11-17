import pandas as pd
import psycopg2
import os
import logging
import time

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def wait_for_postgres(host, database, user, password, max_retries=5):
    """Wait for PostgreSQL to be ready"""
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=host,
                database=database,
                user=user,
                password=password,
                port=5432
            )
            conn.close()
            logger.info(" PostgreSQL is ready!")
            return True
        except Exception as e:
            logger.warning(f" PostgreSQL not ready yet (attempt {i+1}/{max_retries}): {e}")
            time.sleep(5)
    return False

def create_tables():
    """Create necessary tables in PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host='postgres',
            database='bankdb',
            user='admin',
            password='password',
            port=5432
        )
        cur = conn.cursor()
        
        # Raw transactions table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_transactions (
                transaction_id SERIAL PRIMARY KEY,
                customer_id VARCHAR(100),
                customer_location VARCHAR(200),
                account_balance DECIMAL(15,2),
                transaction_date DATE,
                transaction_time BIGINT,
                transaction_amount DECIMAL(15,2),
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info(" Database tables created successfully")
        
    except Exception as e:
        logger.error(f" Error creating tables: {e}")
        raise

def ingest_data():
    """Ingest CSV data into PostgreSQL"""
    try:
        file_path = '/app/data/bank_transactions.csv'
        logger.info(f" Starting data ingestion from {file_path}")
        
        # First, let's check if the file exists and read a small sample
        sample_df = pd.read_csv(file_path, nrows=5)
        logger.info(f" File columns: {sample_df.columns.tolist()}")
        logger.info(f" Sample data shape: {sample_df.shape}")
        
        # Read data in chunks for memory efficiency
        chunk_size = 10000
        total_rows = 0
        
        conn = psycopg2.connect(
            host='postgres',
            database='bankdb',
            user='admin',
            password='password',
            port=5432
        )
        cur = conn.cursor()
        
        for chunk_num, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size)):
            # Clean the data
            chunk = chunk.fillna({
                'CustAccountBalance': 0,
                'TransactionAmount': 0,
                'TransactionTime': 0
            })
            
            # Prepare data for insertion
            data_tuples = []
            for i, (_, row) in enumerate(chunk.iterrows()):
                # Create customer ID if missing
                customer_id = f"cust_{chunk_num}_{i}"
                
                # Handle date conversion
                transaction_date = None
                try:
                    transaction_date = pd.to_datetime(row['TransactionDate']).date()
                except:
                    pass
                
                data_tuples.append((
                    customer_id,
                    str(row.get('CustLocation', 'Unknown'))[:199],  # Limit length
                    float(row.get('CustAccountBalance', 0)),
                    transaction_date,
                    int(row.get('TransactionTime', 0)),
                    float(row.get('TransactionAmount', 0))
                ))
            
            # Insert data
            insert_query = """
                INSERT INTO raw_transactions 
                (customer_id, customer_location, account_balance, transaction_date, transaction_time, transaction_amount)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            cur.executemany(insert_query, data_tuples)
            conn.commit()
            
            total_rows += len(data_tuples)
            logger.info(f" Processed chunk {chunk_num + 1}: {len(data_tuples)} records (Total: {total_rows})")
        
        cur.close()
        conn.close()
        logger.info(f" Data ingestion completed! Total records: {total_rows}")
        
    except Exception as e:
        logger.error(f" Error during data ingestion: {e}")
        raise

if __name__ == "__main__":
    logger.info(" Starting bank data ingestion pipeline...")
    
    # Wait for PostgreSQL to be ready
    if wait_for_postgres('postgres', 'bankdb', 'admin', 'password'):
        create_tables()
        ingest_data()
    else:
        logger.error(" Failed to connect to PostgreSQL after multiple attempts")