#!/usr/bin/env python3
"""
Test CSV structure to understand the data format
"""

import pandas as pd
import os

def test_csv_structure():
    file_path = '../data/bank_transactions.csv'
    
    print("ANALYZING CSV STRUCTURE")
    print("=" * 50)
    
    # Read just the first few rows to understand structure
    df = pd.read_csv(file_path, nrows=10)
    
    print("COLUMNS IN CSV FILE:")
    for i, col in enumerate(df.columns):
        print(f"  {i+1:2d}. '{col}'")
    
    print(f"\nSAMPLE DATA (first 3 rows):")
    print(df.head(3).to_string())
    
    print(f"\nDATA TYPES:")
    print(df.dtypes)
    
    print(f"\nSHAPE: {df.shape}")
    
    # Check specific column samples
    print(f"\nTransactionDate samples: {df['TransactionDate'].head(3).tolist()}")
    print(f"CustAccountBalance samples: {df['CustAccountBalance'].head(3).tolist()}")
    
    # Check transaction amount column name
    amount_col = None
    for col in ['TransactionAmount (INR)', 'TransactionAmount', 'transaction_amount']:
        if col in df.columns:
            amount_col = col
            break
    
    if amount_col:
        print(f"TransactionAmount samples ({amount_col}): {df[amount_col].head(3).tolist()}")
    else:
        print("TransactionAmount column not found! Available columns:")
        for col in df.columns:
            if 'amount' in col.lower() or 'transaction' in col.lower():
                print(f"   - {col}")

if __name__ == "__main__":
    test_csv_structure()