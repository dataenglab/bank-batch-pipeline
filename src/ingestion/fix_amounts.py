import sqlite3
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fix_transaction_amounts():
    """Update transaction amounts with correct values from CSV"""
    import pandas as pd
    
    conn = sqlite3.connect('/app/data/bank_transactions.db')
    cursor = conn.cursor()
    
    # Read the CSV again to get correct amounts
    file_path = '/app/data/bank_transactions.csv'
    
    logger.info(" Fixing transaction amounts...")
    
    chunk_size = 50000
    total_updated = 0
    
    for chunk_num, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size)):
        for i, (_, row) in enumerate(chunk.iterrows()):
            transaction_id = chunk_num * chunk_size + i + 1
            correct_amount = float(row.get('TransactionAmount (INR)', 0))
            
            # Update the record
            cursor.execute(
                "UPDATE raw_transactions SET transaction_amount = ? WHERE transaction_id = ?",
                (correct_amount, transaction_id)
            )
        
        conn.commit()
        total_updated += len(chunk)
        logger.info(f" Updated chunk {chunk_num + 1}: {len(chunk)} records")
    
    logger.info(f" Fixed amounts for {total_updated} records")
    
    # Verify the fix
    cursor.execute("SELECT SUM(transaction_amount) FROM raw_transactions")
    total_amount = cursor.fetchone()[0]
    logger.info(f" Total transaction amount after fix: {total_amount:,.2f}")
    
    conn.close()

if __name__ == "__main__":
    fix_transaction_amounts()