import pandas as pd
import psycopg2
import os
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataIngestion:
    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST', 'postgres'),
            'database': os.getenv('DB_NAME', 'bankdb'),
            'user': os.getenv('DB_USER', 'admin'),
            'password': os.getenv('DB_PASSWORD', 'password'),
            'port': '5432'
        }
        
    def connect_db(self):
        """Establish database connection"""
        return psycopg2.connect(**self.db_config)
    
    def create_tables(self):
        """Create necessary tables"""
        conn = self.connect_db()
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
        
        # Aggregated results table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS quarterly_analytics (
                quarter_id VARCHAR(10),
                customer_id VARCHAR(100),
                total_transactions INTEGER,
                total_amount DECIMAL(15,2),
                avg_transaction_amount DECIMAL(15,2),
                last_balance DECIMAL(15,2),
                customer_segment VARCHAR(50),
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (quarter_id, customer_id)
            )
        """)
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Database tables created successfully")
    
    def ingest_data(self, file_path):
        """Ingest CSV data into database"""
        try:
            # Read data in chunks for memory efficiency
            chunk_size = 50000
            conn = self.connect_db()
            cur = conn.cursor()
            
            logger.info(f"Starting data ingestion from {file_path}")
            
            for chunk_num, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size)):
                # Convert timestamp - handle different date formats
                chunk['TransactionDate'] = pd.to_datetime(chunk['TransactionDate'], errors='coerce')
                
                # Prepare data for insertion
                data_tuples = []
                for i, (_, row) in enumerate(chunk.iterrows()):
                    # Handle missing CustomerID
                    customer_id = row.get('CustomerID', f"cust_{chunk_num}_{i}")
                    
                    data_tuples.append((
                        str(customer_id),
                        str(row.get('CustLocation', 'Unknown')),
                        float(row.get('CustAccountBalance', 0)),
                        row['TransactionDate'].date() if pd.notna(row.get('TransactionDate')) else None,
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
                
                logger.info(f"Processed chunk {chunk_num + 1}: {len(data_tuples)} records")
            
            cur.close()
            conn.close()
            logger.info("Data ingestion completed successfully")
            
        except Exception as e:
            logger.error(f"Error during data ingestion: {str(e)}")
            raise

if __name__ == "__main__":
    ingestion = DataIngestion()
    ingestion.create_tables()
    ingestion.ingest_data('/app/data/bank_transactions.csv')