#!/usr/bin/env python3
"""
Processor with fixed 2-digit year interpretation
"""

import pandas as pd
import psycopg2
from datetime import datetime

print(" PROCESSOR WITH YEAR FIX")

def fix_two_digit_year(date_str, format_str):
    """Fix 2-digit year interpretation"""
    try:
        parsed = pd.to_datetime(date_str, format=format_str)
        if parsed.year > 2020:
            fixed_year = parsed.year - 100
            parsed = parsed.replace(year=fixed_year)
        return parsed.date()
    except:
        return None

def main():
    conn = psycopg2.connect(
        host='postgres', port=5432, database='bank_data', 
        user='admin', password='password123'
    )
    cursor = conn.cursor()

    total_stored = 0
    chunk_size = 100000

    for chunk_num, chunk in enumerate(pd.read_csv('/app/data/bank_transactions.csv', chunksize=chunk_size), 1):
        if total_stored >= 100000:
            print('Test limit reached')
            break
            
        stored_in_chunk = 0
        print(f'Chunk {chunk_num}: {len(chunk):,} records')
        
        for idx, row in chunk.iterrows():
            try:
                # Convert dates with FIXED year interpretation
                trans_date = fix_two_digit_year(row['TransactionDate'], '%m/%d/%y')
                cust_dob = fix_two_digit_year(row['CustomerDOB'], '%d/%m/%y')
                
                if not trans_date:
                    continue
                    
                # Convert numeric fields
                balance = float(row['CustAccountBalance'])
                amount = float(row['TransactionAmount (INR)'])
                
                cursor.execute('''
                    INSERT INTO transactions
                    (transaction_id, customer_id, customer_dob, cust_gender, cust_location,
                     cust_account_balance, transaction_date, transaction_time, transaction_amount)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (transaction_id) DO NOTHING
                ''', (
                    str(row['TransactionID']), str(row['CustomerID']), cust_dob,
                    str(row['CustGender'])[:1], str(row['CustLocation']),
                    balance, trans_date, str(row['TransactionTime']), amount
                ))
                stored_in_chunk += 1
                
            except Exception as e:
                continue
        
        conn.commit()
        total_stored += stored_in_chunk
        print(f'  Stored: {stored_in_chunk:,} records')
        print(f'  Success rate: {stored_in_chunk/len(chunk)*100:.1f}%')
        print(f'  Total: {total_stored:,} stored\\n')

    print(f' FINAL: {total_stored:,} records stored')
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()