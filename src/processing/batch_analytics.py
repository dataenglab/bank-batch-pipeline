import sqlite3
import pandas as pd
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_quarterly_analytics():
    """Run batch analytics on the ingested data"""
    conn = sqlite3.connect('/app/data/bank_transactions.db')
    
    logger.info(" Running Quarterly Customer Analytics...")
    
    # Create analytics table
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS quarterly_analytics (
            quarter TEXT,
            customer_id TEXT,
            total_transactions INTEGER,
            total_amount REAL,
            avg_transaction REAL,
            customer_segment TEXT,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (quarter, customer_id)
        )
    ''')
    
    # Quarterly aggregation query
    analytics_query = '''
        INSERT INTO quarterly_analytics (quarter, customer_id, total_transactions, total_amount, avg_transaction, customer_segment)
        SELECT 
            '2023-Q1' as quarter,
            customer_id,
            COUNT(*) as total_transactions,
            SUM(transaction_amount) as total_amount,
            AVG(transaction_amount) as avg_transaction,
            CASE 
                WHEN SUM(transaction_amount) > 50000 THEN 'High Value'
                WHEN SUM(transaction_amount) > 10000 THEN 'Medium Value' 
                ELSE 'Standard'
            END as customer_segment
        FROM raw_transactions
        GROUP BY customer_id
    '''
    
    cursor.execute(analytics_query)
    conn.commit()
    
    logger.info(" Quarterly analytics computed successfully!")
    
    # Show results
    results = pd.read_sql_query("""
        SELECT 
            customer_segment,
            COUNT(*) as customer_count,
            AVG(total_amount) as avg_total_amount,
            SUM(total_amount) as segment_total
        FROM quarterly_analytics 
        GROUP BY customer_segment
        ORDER BY segment_total DESC
    """, conn)
    
    print("\n Customer Segmentation Results:")
    print(results.to_string(index=False))
    
    # Show time-based analytics
    time_analytics = pd.read_sql_query("""
        SELECT 
            COUNT(*) as total_transactions,
            SUM(transaction_amount) as total_amount,
            AVG(transaction_amount) as avg_amount,
            MIN(transaction_date) as earliest_date,
            MAX(transaction_date) as latest_date
        FROM raw_transactions
    """, conn)
    
    print("\n Time-Based Analytics:")
    print(time_analytics.to_string(index=False))
    
    conn.close()

if __name__ == "__main__":
    run_quarterly_analytics()