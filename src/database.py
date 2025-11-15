import psycopg2
import os
from datetime import datetime
import time

class DatabaseClient:
    def __init__(self):
        self.conn = None
        self._connect_with_retry()
    
    def _connect_with_retry(self):
        """Connect to PostgreSQL with retry logic for container startup"""
        max_retries = 30
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                self.conn = psycopg2.connect(
                    host=os.getenv('POSTGRES_HOST', 'postgres'),
                    database=os.getenv('POSTGRES_DB', 'bank_data'),
                    user=os.getenv('POSTGRES_USER', 'admin'),
                    password=os.getenv('POSTGRES_PASSWORD', 'password123'),
                    connect_timeout=5
                )
                print(f" Database connection successful (attempt {attempt + 1})")
                return
                
            except psycopg2.OperationalError as e:
                if attempt < max_retries - 1:
                    print(f" Database not ready, retrying in {retry_delay}s... (attempt {attempt + 1})")
                    time.sleep(retry_delay)
                else:
                    print(f" Failed to connect to database after {max_retries} attempts")
                    raise
    
    def insert_transaction_batch(self, transactions):
        """
        Insert a batch of transactions and return the actual number inserted
        """
        if not transactions:
            return 0
            
        try:
            cursor = self.conn.cursor()
            
            insert_query = """
                INSERT INTO transactions 
                (transaction_id, customer_id, transaction_amount, transaction_date, transaction_time, processed_at)
                VALUES (%(transaction_id)s, %(customer_id)s, %(transaction_amount)s, %(transaction_date)s, %(transaction_time)s, %(processed_at)s)
            """
            
            # Add processed_at timestamp if not present
            for transaction in transactions:
                if 'processed_at' not in transaction:
                    transaction['processed_at'] = datetime.now()
            
            cursor.executemany(insert_query, transactions)
            self.conn.commit()
            
            # DEBUG: Log the actual insert
            print(f"DEBUG: Successfully inserted {len(transactions)} records")
            
            return len(transactions)
            
        except psycopg2.IntegrityError as e:
            # Handle duplicate key errors
            print(f" Integrity error (possibly duplicates): {e}")
            self.conn.rollback()
            return 0
        except psycopg2.Error as e:
            print(f" Database error in batch insert: {e}")
            self.conn.rollback()
            return 0
        except Exception as e:
            print(f" Unexpected error in batch insert: {e}")
            self.conn.rollback()
            return 0
        finally:
            if 'cursor' in locals():
                cursor.close()
    
    def get_transaction_count(self):
        """Get the total number of records in transactions table"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM transactions")
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except Exception as e:
            print(f" Count error: {e}")
            return 0
    
    def test_connection(self):
        """Test if database connection is working"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            cursor.close()
            return result[0] == 1
        except Exception as e:
            print(f" Connection test failed: {e}")
            return False
    
    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
            print(" Database connection closed")

# Test the connection if run directly
if __name__ == "__main__":
    print("Testing database connection...")
    db = DatabaseClient()
    
    if db.test_connection():
        print(" Database connection test passed!")
        count = db.get_transaction_count()
        print(f" Current records in transactions table: {count}")
    else:
        print(" Database connection test failed!")
    
    db.close()