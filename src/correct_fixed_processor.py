#!/usr/bin/env python3
"""
Correctly Fixed Processor with proper date format handling
"""

import pandas as pd
import psycopg2
from datetime import datetime

print(" CORRECTLY FIXED PROCESSOR STARTING")

class CorrectFixedProcessor:
    def __init__(self):
        self.setup_storage()
        
    def setup_storage(self):
        print(" Setting up storage...")
        self.pg_conn = psycopg2.connect(
            host='postgres', port=5432, database='bank_data', 
            user='admin', password='password123'
        )
        print(" PostgreSQL connected")

    def convert_transaction_date(self, date_str):
        """Convert TransactionDate (M/D/YY format)"""
        if pd.isna(date_str) or not date_str:
            return None
            
        date_str = str(date_str).strip()
        
        # TransactionDate is in M/D/YY format (e.g., '2/8/16')
        try:
            parsed = pd.to_datetime(date_str, format='%m/%d/%y')
            if 2000 <= parsed.year <= 2020:
                return parsed.date()
        except:
            pass
            
        return None

    def convert_customer_dob(self, date_str):
        """Convert CustomerDOB (D/M/YY format)"""
        if pd.isna(date_str) or not date_str:
            return None
            
        date_str = str(date_str).strip()
        
        # CustomerDOB is in D/M/YY format (e.g., '10/1/94', '26/11/96')
        try:
            parsed = pd.to_datetime(date_str, format='%d/%m/%y')
            if 1900 <= parsed.year <= 2010:  # DOBs are older
                return parsed.date()
        except:
            pass
            
        return None

    def process_chunk(self, chunk, chunk_number):
        print(f" Processing Chunk {chunk_number}...")
        print(f"  Records: {len(chunk):,}")
        
        cursor = self.pg_conn.cursor()
        stored = 0
        date_errors = 0
        numeric_errors = 0
        other_errors = 0
        
        for idx, row in chunk.iterrows():
            try:
                # Convert dates with CORRECT formats
                trans_date = self.convert_transaction_date(row['TransactionDate'])
                
                if not trans_date:
                    date_errors += 1
                    continue
                    
                cust_dob = self.convert_customer_dob(row['CustomerDOB'])
                
                # Convert numeric fields  
                try:
                    balance = float(row['CustAccountBalance'])
                    amount = float(row['TransactionAmount (INR)'])
                except:
                    numeric_errors += 1
                    continue
                
                # Insert with proper date handling
                cursor.execute("""
                    INSERT INTO transactions
                    (transaction_id, customer_id, customer_dob, cust_gender, cust_location,
                     cust_account_balance, transaction_date, transaction_time, transaction_amount)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (transaction_id) DO NOTHING
                """, (
                    str(row['TransactionID']), 
                    str(row['CustomerID']),
                    cust_dob or datetime(1900, 1, 1).date(),  # Default for missing DOB
                    str(row['CustGender'])[:1] if pd.notna(row['CustGender']) else 'U',
                    str(row['CustLocation']) if pd.notna(row['CustLocation']) else 'Unknown',
                    balance, 
                    trans_date, 
                    str(row['TransactionTime']) if pd.notna(row['TransactionTime']) else '000000',
                    amount
                ))
                
                stored += 1
                if stored <= 3:  # Show first few successes
                    print(f"   Sample: {row['TransactionID']} -> TransDate: {trans_date}, DOB: {cust_dob}")
                    
            except Exception as e:
                other_errors += 1
                continue
        
        self.pg_conn.commit()
        cursor.close()
        
        print(f"   Stored: {stored:,} records")
        print(f"    Skipped: {date_errors + numeric_errors + other_errors:,} rows")
        print(f"     - Date errors: {date_errors:,}")
        print(f"     - Numeric errors: {numeric_errors:,}") 
        print(f"     - Other errors: {other_errors:,}")
        print(f"   Success rate: {stored/len(chunk)*100:.1f}%")
        
        return stored

    def process(self):
        print(" PROCESSING STARTED")
        print("=" * 50)
        
        total_processed = 0
        total_stored = 0
        
        for chunk_num, chunk in enumerate(pd.read_csv('/app/data/bank_transactions.csv', chunksize=100000), 1):
            if total_processed >= 300000:  # Demo limit
                print(f"  Demo limit reached (300k records)")
                break
                
            stored = self.process_chunk(chunk, chunk_num)
            total_processed += len(chunk)
            total_stored += stored
            
            print(f" Running total: {total_stored:,} stored / {total_processed:,} processed\n")
        
        print(f" COMPLETED!")
        print(f" Final: {total_stored:,} records stored out of {total_processed:,} processed")
        print(f" Success rate: {total_stored/total_processed*100:.2f}%")
        
        self.pg_conn.close()

if __name__ == "__main__":
    processor = CorrectFixedProcessor()
    processor.process()