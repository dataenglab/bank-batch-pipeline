#!/usr/bin/env python3
"""
Minimal working batch processor
"""

import pandas as pd
import psycopg2
from datetime import datetime

print("MINIMAL BATCH PROCESSOR STARTING")

# Database configuration
DB_CONFIG = {
    'host': 'postgres',
    'port': 5432,
    'database': 'bank_data',
    'user': 'admin',
    'password': 'password123'
}

def convert_date_smart(date_str):
    """Smart date conversion"""
    if pd.isna(date_str) or not date_str:
        return None
    
    date_str = str(date_str).strip()
    
    # Try common formats
    formats = ['%m/%d/%y', '%d/%m/%y', '%Y-%m-%d']
    
    for fmt in formats:
        try:
            parsed = pd.to_datetime(date_str, format=fmt)
            # Validate reasonable year
            if 2000 <= parsed.year <= 2020:
                return parsed.date()
        except:
            continue
    
    return None

try:
    # Connect to database
    print("Connecting to PostgreSQL...")
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Process file in chunks
    file_path = '/app/data/bank_transactions.csv'
    chunk_size = 100000
    total_processed = 0
    total_stored = 0
    
    print(f"Processing {file_path}...")
    
    for chunk_num, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size), 1):
        if total_processed >= 300000:  # Demo limit
            print(f"Demo limit reached (300k records)")
            break
            
        print(f"Chunk {chunk_num}: {len(chunk):,} records")
        
        stored_in_chunk = 0
        for idx, row in chunk.iterrows():
            try:
                # Convert dates
                trans_date = convert_date_smart(row['TransactionDate'])
                cust_dob = convert_date_smart(row['CustomerDOB'])
                
                if not trans_date:
                    continue
                
                # Convert numeric fields
                try:
                    balance = float(row['CustAccountBalance'])
                    amount = float(row['TransactionAmount (INR)'])
                except:
                    continue
                
                # Insert into database
                cursor.execute("""
                    INSERT INTO transactions 
                    (transaction_id, customer_id, customer_dob, cust_gender, cust_location,
                     cust_account_balance, transaction_date, transaction_time, transaction_amount)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (transaction_id) DO NOTHING
                """, (
                    str(row['TransactionID']),
                    str(row['CustomerID']),
                    cust_dob,
                    str(row['CustGender'])[:1] if pd.notna(row['CustGender']) else 'U',
                    str(row['CustLocation']) if pd.notna(row['CustLocation']) else 'Unknown',
                    balance,
                    trans_date,
                    str(row['TransactionTime']) if pd.notna(row['TransactionTime']) else '000000',
                    amount
                ))
                
                stored_in_chunk += 1
                
            except Exception as e:
                continue
        
        conn.commit()
        total_processed += len(chunk)
        total_stored += stored_in_chunk
        
        print(f"  Stored: {stored_in_chunk:,} records")
        print(f"  Running total: {total_stored:,} stored out of {total_processed:,} processed")
    
    print(f"\nFINAL: {total_stored:,} records stored out of {total_processed:,} processed")
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()