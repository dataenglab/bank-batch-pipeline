#!/usr/bin/env python3
"""
Robust processor that handles both 2-digit and 4-digit years
"""

import pandas as pd
import psycopg2
import re
from datetime import datetime

print(" ROBUST PROCESSOR WITH MIXED YEAR SUPPORT")

def parse_transaction_date(date_str):
    """Parse TransactionDate (M/D/YY or M/D/YYYY format)"""
    if pd.isna(date_str) or not date_str:
        return None
        
    date_str = str(date_str).strip()
    
    # Try different formats for TransactionDate
    formats = [
        '%m/%d/%y',  # M/D/YY (most common)
        '%m/%d/%Y',  # M/D/YYYY
        '%d/%m/%y',  # D/M/YY (fallback)
        '%d/%m/%Y',  # D/M/YYYY (fallback)
    ]
    
    for fmt in formats:
        try:
            parsed = pd.to_datetime(date_str, format=fmt)
            # Fix 2-digit year interpretation
            if fmt.endswith('/%y') and parsed.year > 2020:
                fixed_year = parsed.year - 100
                parsed = parsed.replace(year=fixed_year)
            # Validate transaction date range
            if 2000 <= parsed.year <= 2020:
                return parsed.date()
        except:
            continue
    
    return None

def parse_customer_dob(date_str):
    """Parse CustomerDOB (D/M/YY or D/M/YYYY format)"""
    if pd.isna(date_str) or not date_str:
        return None
        
    date_str = str(date_str).strip()
    
    # Try different formats for CustomerDOB
    formats = [
        '%d/%m/%y',  # D/M/YY (most common)
        '%d/%m/%Y',  # D/M/YYYY  
        '%m/%d/%y',  # M/D/YY (fallback)
        '%m/%d/%Y',  # M/D/YYYY (fallback)
    ]
    
    for fmt in formats:
        try:
            parsed = pd.to_datetime(date_str, format=fmt)
            # Fix 2-digit year interpretation
            if fmt.endswith('/%y') and parsed.year > 2020:
                fixed_year = parsed.year - 100
                parsed = parsed.replace(year=fixed_year)
            # Validate DOB range (reasonable ages)
            if 1800 <= parsed.year <= 2010:
                return parsed.date()
        except:
            continue
    
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
                # Use robust date parsers
                trans_date = parse_transaction_date(row['TransactionDate'])
                cust_dob = parse_customer_dob(row['CustomerDOB'])
                
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
        print(f'  Total: {total_stored:,} stored\n')

    print(f' FINAL: {total_stored:,} records stored')
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()