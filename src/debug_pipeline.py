# src/debug_pipeline.py
import sys
import os
import time
import random
from datetime import datetime

# Import your advanced features
from data_validator import DataValidator
from error_handler import ErrorHandler

class DebugPipeline:
    def __init__(self):
        self.validator = DataValidator()
        self.error_handler = ErrorHandler('debug_pipeline_errors.log')
        self.processed_count = 0
        self.start_time = time.time()
    
    def generate_sample_data(self, count=10):
        """Generate sample transaction data for testing"""
        sample_data = []
        currencies = ['USD', 'EUR', 'GBP', 'INVALID']
        
        for i in range(count):
            # Create more valid transactions for testing
            if i % 4 == 0:  # Only 25% invalid now
                transaction = {
                    'transaction_id': f'TX{i:04d}',
                    'account_id': '' if i % 2 == 0 else f'ACC{i:05d}',
                    'amount': -random.uniform(10, 1000) if i % 3 == 0 else random.uniform(10, 2000000),
                    'currency': random.choice(currencies),
                    'timestamp': 'invalid_date' if i % 4 == 0 else datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            else:
                # Generate definitely valid transactions
                transaction = {
                    'transaction_id': f'TX{i:04d}',
                    'account_id': f'ACC{i:05d}',
                    'amount': random.uniform(10, 1000),  # Reasonable amount
                    'currency': random.choice(['USD', 'EUR', 'GBP']),  # Valid currency
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Valid timestamp
                }
            
            sample_data.append(transaction)
        
        return sample_data
    
    def debug_validation(self, transaction):
        """Debug why validation is failing"""
        checks = {
            'amount_positive': transaction.get('amount', 0) > 0,
            'valid_timestamp': self._is_valid_timestamp(transaction.get('timestamp')),
            'account_not_empty': bool(transaction.get('account_id')),
            'reasonable_amount': transaction.get('amount', 0) <= 1000000,
            'valid_currency': transaction.get('currency') in ['USD', 'EUR', 'GBP']
        }
        
        print(f"Validating transaction {transaction['transaction_id']}:")
        for check_name, check_result in checks.items():
            status = "PASS" if check_result else "FAIL"
            print(f"  {check_name}: {status}")
            
        return all(checks.values())
    
    def _is_valid_timestamp(self, timestamp):
        """Validate timestamp is reasonable"""
        try:
            if not timestamp:
                return False
            ts = pd.to_datetime(timestamp)
            return ts.year >= 2020 and ts <= datetime.now()
        except:
            return False
    
    def process_transactions_debug(self, transactions):
        """Process transactions with detailed debugging"""
        print("=== DEBUG PIPELINE PROCESSING ===")
        print(f"Processing {len(transactions)} transactions...")
        
        valid_transactions = []
        
        for i, transaction in enumerate(transactions):
            print(f"\n--- Transaction {i+1}/{len(transactions)} ---")
            print(f"Data: {transaction}")
            
            # Debug validation
            is_valid = self.debug_validation(transaction)
            print(f"Overall validation: {'PASS' if is_valid else 'FAIL'}")
            
            if not is_valid:
                print("Skipping invalid transaction")
                continue
            
            # If valid, try to process
            try:
                # Simulate processing
                def process_operation():
                    # Lower chance of error for debugging
                    if random.random() < 0.05:  # Only 5% chance of error
                        raise ConnectionError("Database connection timeout")
                    return f"Processed {transaction['transaction_id']}"
                
                # Process with error handling
                result = self.error_handler.handle_error(
                    'processing_error',
                    f"Failed to process {transaction['transaction_id']}",
                    {'transaction': transaction, 'attempt': i},
                    process_operation
                )
                
                if result:
                    valid_transactions.append(transaction)
                    self.processed_count += 1
                    print(f"SUCCESS: {result}")
                else:
                    print(f"FAILED: Could not process {transaction['transaction_id']}")
                    
            except Exception as e:
                self.error_handler.handle_error(
                    'unexpected_error',
                    f"Unexpected error processing transaction: {str(e)}",
                    {'transaction': transaction}
                )
                print(f"EXCEPTION: {e}")
        
        return valid_transactions
    
    def run_debug(self, transaction_count=10):
        """Run debug version with detailed output"""
        print("STARTING DEBUG PIPELINE")
        print("=" * 50)
        
        # Generate sample data
        transactions = self.generate_sample_data(transaction_count)
        print(f"Generated {len(transactions)} sample transactions")
        
        # Process transactions with debugging
        valid_transactions = self.process_transactions_debug(transactions)
        print(f"\nFinal result: {len(valid_transactions)} valid transactions processed")
        
        return valid_transactions

if __name__ == '__main__':
    # Add pandas import at the top if needed
    import pandas as pd
    
    # Run the debug pipeline
    pipeline = DebugPipeline()
    pipeline.run_debug(10)