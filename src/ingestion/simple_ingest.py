import pandas as pd
import sqlite3
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def explore_dataset():
    """First, let's explore the dataset structure"""
    file_path = '/app/data/bank_transactions.csv'
    
    logger.info(" Exploring dataset...")
    
    # Check file size
    file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
    logger.info(f" File size: {file_size:.2f} MB")
    
    # Read just the first few rows to understand structure
    df_sample = pd.read_csv(file_path, nrows=10)
    logger.info(f" Columns: {df_sample.columns.tolist()}")
    logger.info(f" Shape: {df_sample.shape}")
    logger.info("\n First 3 rows:")
    print(df_sample.head(3))
    
    return df_sample.columns.tolist()

def create_sqlite_database():
    """Create SQLite database and tables"""
    conn = sqlite3.connect('/app/data/bank_transactions.db')
    cursor = conn.cursor()
    
    # Create table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS raw_transactions (
            transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id TEXT,
            customer_location TEXT,
            account_balance REAL,
            transaction_date TEXT,
            transaction_time INTEGER,
            transaction_amount REAL,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.commit()
    logger.info(" SQLite database and table created successfully")
    return conn

def ingest_data_in_chunks():
    """Ingest data in chunks to avoid memory issues"""
    file_path = '/app/data/bank_transactions.csv'
    conn = create_sqlite_database()
    cursor = conn.cursor()
    
    chunk_size = 50000
    total_rows = 0
    
    try:
        logger.info(" Starting data ingestion...")
        
        for chunk_num, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size)):
            # Clean and prepare data
            chunk_cleaned = chunk.fillna({
                'CustAccountBalance': 0,
                'TransactionAmount (INR)': 0,
                'TransactionTime': 0
            })
            
            # Convert to records
            records = []
            for _, row in chunk_cleaned.iterrows():
                customer_id = str(row.get('CustomerID', f'cust_{chunk_num}_{total_rows}'))
                
                # FIXED: Use correct column name for transaction amount
                transaction_amount = float(row.get('TransactionAmount (INR)', 0))
                
                records.append((
                    customer_id,
                    str(row.get('CustLocation', 'Unknown')),
                    float(row.get('CustAccountBalance', 0)),
                    str(row.get('TransactionDate', '')),
                    int(row.get('TransactionTime', 0)),
                    transaction_amount  # CORRECTED
                ))
                total_rows += 1
            
            # Insert into database
            cursor.executemany('''
                INSERT INTO raw_transactions 
                (customer_id, customer_location, account_balance, transaction_date, transaction_time, transaction_amount)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', records)
            
            conn.commit()
            logger.info(f" Processed chunk {chunk_num + 1}: {len(records)} records (Total: {total_rows})")
            
            # Show progress every 5 chunks
            if chunk_num % 5 == 0:
                cursor.execute("SELECT COUNT(*) FROM raw_transactions")
                current_count = cursor.fetchone()[0]
                logger.info(f" Current database count: {current_count}")
        
        logger.info(f" Data ingestion completed! Total records processed: {total_rows}")
        
        # Final count
        cursor.execute("SELECT COUNT(*) FROM raw_transactions")
        final_count = cursor.fetchone()[0]
        logger.info(f" Final database count: {final_count}")
        
    except Exception as e:
        logger.error(f" Error during ingestion: {e}")
        raise
    finally:
        conn.close()

def verify_data():
    """Verify the ingested data"""
    conn = sqlite3.connect('/app/data/bank_transactions.db')
    
    # Check basic stats
    queries = {
        "Total Records": "SELECT COUNT(*) FROM raw_transactions",
        "Unique Customers": "SELECT COUNT(DISTINCT customer_id) FROM raw_transactions",
        "Total Transaction Amount": "SELECT SUM(transaction_amount) FROM raw_transactions",
        "Average Transaction": "SELECT AVG(transaction_amount) FROM raw_transactions",
        "Max Transaction": "SELECT MAX(transaction_amount) FROM raw_transactions"
    }
    
    logger.info(" Verifying ingested data...")
    
    for description, query in queries.items():
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()[0]
        logger.info(f" {description}: {result}")
    
    # Show sample data
    cursor.execute("SELECT * FROM raw_transactions LIMIT 3")
    sample_data = cursor.fetchall()
    logger.info(" Sample records:")
    for row in sample_data:
        logger.info(f"   {row}")
    
    conn.close()

if __name__ == "__main__":
    logger.info(" Starting Bank Data Processing Pipeline")
    
    # Step 1: Explore the dataset
    columns = explore_dataset()
    
    # Step 2: Ingest data (WITH CORRECTED COLUMN NAME)
    ingest_data_in_chunks()
    
    # Step 3: Verify the data
    verify_data()
    
    logger.info(" Pipeline completed successfully!")