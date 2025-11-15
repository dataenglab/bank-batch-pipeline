#!/usr/bin/env python3
import json
import random
import uuid
from datetime import datetime, timedelta
import logging

class EnhancedDataGenerator:
    def __init__(self):
        self.transaction_id = 1000
        self.logger = logging.getLogger(__name__)
    
    def generate_transaction(self):
        """Generate a single realistic banking transaction"""
        transaction_types = ['TRANSFER', 'PAYMENT', 'WITHDRAWAL', 'DEPOSIT', 'PURCHASE']
        currencies = ['USD', 'EUR', 'GBP', 'JPY']
        
        # Generate random amount with realistic distribution
        base_amount = random.uniform(1.0, 5000.0)
        amount = round(base_amount, 2)
        
        # Generate timestamp within last 30 days
        days_ago = random.randint(0, 30)
        hours_ago = random.randint(0, 23)
        minutes_ago = random.randint(0, 59)
        
        timestamp = (datetime.now() - 
                    timedelta(days=days_ago, hours=hours_ago, minutes=minutes_ago))
        
        transaction = {
            'transaction_id': f"TXN{self.transaction_id:08d}",
            'amount': amount,
            'currency': random.choice(currencies),
            'timestamp': timestamp.isoformat(),
            'account_id': f"ACC{random.randint(10000, 99999)}",
            'recipient_account_id': f"ACC{random.randint(10000, 99999)}",
            'transaction_type': random.choice(transaction_types),
            'description': f"Transaction {self.transaction_id}",
            'status': random.choice(['COMPLETED', 'PENDING', 'PROCESSING']),
            'category': random.choice(['Shopping', 'Bills', 'Food', 'Transport', 'Entertainment']),
            'merchant_name': f"Merchant_{random.randint(1, 1000)}",
            'location': f"City_{random.randint(1, 100)}"
        }
        
        self.transaction_id += 1
        return transaction
    
    def generate_batch(self, batch_size=100):
        """Generate a batch of transactions"""
        self.logger.info(f"Generating batch of {batch_size} transactions")
        
        transactions = []
        for i in range(batch_size):
            transaction = self.generate_transaction()
            transactions.append(transaction)
            
            if (i + 1) % 10 == 0:
                self.logger.debug(f"Generated {i + 1}/{batch_size} transactions")
        
        self.logger.info(f"Successfully generated {len(transactions)} transactions")
        return transactions
    
    def save_to_file(self, transactions, filename=None):
        """Save transactions to a JSON file"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"generated_transactions_{timestamp}.json"
        
        try:
            with open(filename, 'w') as f:
                json.dump(transactions, f, indent=2)
            self.logger.info(f"Transactions saved to {filename}")
            return filename
        except Exception as e:
            self.logger.error(f"Error saving to file {filename}: {e}")
            return None

def main():
    """Test function for the enhanced data generator"""
    logging.basicConfig(level=logging.INFO)
    generator = EnhancedDataGenerator()
    
    print("Enhanced Data Generator Test")
    print("=" * 40)
    
    # Generate a small test batch
    transactions = generator.generate_batch(5)
    
    print("Generated Transactions:")
    print("-" * 40)
    
    for i, transaction in enumerate(transactions, 1):
        print(f"Transaction {i}:")
        print(f"  ID: {transaction['transaction_id']}")
        print(f"  Amount: {transaction['amount']} {transaction['currency']}")
        print(f"  Type: {transaction['transaction_type']}")
        print(f"  Account: {transaction['account_id']}")
        print(f"  Timestamp: {transaction['timestamp']}")
        print(f"  Category: {transaction['category']}")
        print()

if __name__ == "__main__":
    main()