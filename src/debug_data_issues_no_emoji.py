#!/usr/bin/env python3
"""
Debug script to identify why records are being skipped
"""

import pandas as pd
import os

def analyze_data_issues():
    print("ANALYZING DATA QUALITY ISSUES")
    print("=" * 50)
    
    # Read first 1000 rows to analyze
    file_path = '../data/bank_transactions.csv'
    df = pd.read_csv(file_path, nrows=1000)
    
    print(f"Total records sampled: {len(df)}")
    print(f"Columns: {list(df.columns)}")
    print()
    
    # Check for null values
    print("NULL VALUES PER COLUMN:")
    null_counts = df.isnull().sum()
    for col, count in null_counts.items():
        if count > 0:
            print(f"   {col}: {count} nulls ({count/len(df)*100:.1f}%)")
    
    print()
    print("DATE FORMAT ANALYSIS:")
    # Check TransactionDate format
    date_sample = df['TransactionDate'].head(10)
    print("   Sample dates:", date_sample.tolist())
    
    # Try to parse dates with the original format
    date_errors = 0
    successful_dates = []
    for date_str in df['TransactionDate']:
        try:
            parsed_date = pd.to_datetime(date_str, format='%d/%m/%y')
            successful_dates.append(parsed_date)
        except:
            date_errors += 1
    
    print(f"   Date parsing errors: {date_errors}/{len(df)} ({date_errors/len(df)*100:.1f}%)")
    if successful_dates:
        print(f"   First successful date: {successful_dates[0]}")
    
    print()
    print("NUMERIC FIELD ANALYSIS:")
    # Check numeric fields
    numeric_fields = ['CustAccountBalance', 'TransactionAmount (INR)']
    for field in numeric_fields:
        if field in df.columns:
            # Check if can be converted to numeric
            converted = pd.to_numeric(df[field], errors='coerce')
            errors = converted.isna().sum()
            print(f"   {field}: {errors} conversion errors ({errors/len(df)*100:.1f}%)")
            print(f"   Sample values: {df[field].head(5).tolist()}")
            print(f"   Min value: {converted.min()}, Max value: {converted.max()}")

if __name__ == "__main__":
    analyze_data_issues()