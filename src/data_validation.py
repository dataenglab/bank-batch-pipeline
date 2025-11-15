#!/usr/bin/env python3
import pandas as pd
import numpy as np
from datetime import datetime
import logging

class AdvancedDataValidator:
    def __init__(self):
        self.required_fields = ['transaction_id', 'amount', 'timestamp', 'account_id']
        self.logger = logging.getLogger(__name__)
    
    def validate_transaction(self, transaction):
        """Validate a single transaction"""
        try:
            # Check required fields
            if not all(field in transaction for field in self.required_fields):
                self.logger.warning(f"Missing required fields in transaction: {transaction}")
                return False
            
            # Validate amount
            try:
                amount = float(transaction['amount'])
                if amount <= 0:
                    self.logger.warning(f"Invalid amount: {amount}")
                    return False
            except (ValueError, TypeError):
                self.logger.warning(f"Invalid amount format: {transaction['amount']}")
                return False
            
            # Validate timestamp
            try:
                datetime.fromisoformat(transaction['timestamp'].replace('Z', '+00:00'))
            except (ValueError, TypeError):
                self.logger.warning(f"Invalid timestamp: {transaction['timestamp']}")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Validation error for transaction {transaction}: {e}")
            return False
    
    def validate_batch(self, transactions):
        """Validate a batch of transactions"""
        if not transactions:
            self.logger.warning("Empty batch received for validation")
            return []
        
        valid_transactions = []
        for transaction in transactions:
            if self.validate_transaction(transaction):
                valid_transactions.append(transaction)
        
        self.logger.info(f"Validation complete: {len(valid_transactions)}/{len(transactions)} valid")
        return valid_transactions
    
    def generate_validation_report(self, transactions):
        """Generate a detailed validation report"""
        report = {
            'total_transactions': len(transactions),
            'valid_transactions': 0,
            'invalid_transactions': 0,
            'validation_errors': []
        }
        
        for i, transaction in enumerate(transactions):
            is_valid = self.validate_transaction(transaction)
            if is_valid:
                report['valid_transactions'] += 1
            else:
                report['invalid_transactions'] += 1
                report['validation_errors'].append({
                    'index': i,
                    'transaction': transaction,
                    'reason': 'Failed validation checks'
                })
        
        return report

def main():
    """Test function for the data validator"""
    logging.basicConfig(level=logging.INFO)
    validator = AdvancedDataValidator()
    
    # Test data
    test_transactions = [
        {
            'transaction_id': 'T001',
            'amount': 100.0,
            'timestamp': '2024-01-15T10:30:00',
            'account_id': 'A123'
        },
        {
            'transaction_id': 'T002', 
            'amount': -50.0,  # Invalid amount
            'timestamp': '2024-01-15T11:00:00',
            'account_id': 'A124'
        },
        {
            'transaction_id': 'T003',
            'amount': 200.0,
            'timestamp': 'invalid_date',  # Invalid timestamp
            'account_id': 'A125'
        }
    ]
    
    print("Testing Data Validation System")
    print("=" * 40)
    
    # Generate validation report
    report = validator.generate_validation_report(test_transactions)
    
    print(f"Total transactions: {report['total_transactions']}")
    print(f"Valid transactions: {report['valid_transactions']}")
    print(f"Invalid transactions: {report['invalid_transactions']}")
    
    if report['validation_errors']:
        print("\nValidation Errors:")
        for error in report['validation_errors']:
            print(f"  Index {error['index']}: {error['transaction']}")

if __name__ == "__main__":
    main()