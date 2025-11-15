import random
import uuid
from datetime import datetime, timedelta

class DataGenerator:
    def __init__(self):
        # Initialize random seed for better randomness
        random.seed()
    
    def generate_batch(self, size):
        batch = []
        for i in range(size):
            # Use UUID for truly unique transaction IDs
            transaction_id = f'tx_{uuid.uuid4().hex[:8]}_{int(datetime.now().timestamp() * 1000)}'
            
            transaction = {
                'transaction_id': transaction_id,
                'customer_id': f'cust_{random.randint(1000, 9999)}',
                'transaction_amount': round(random.uniform(1.0, 1000.0), 2),
                'transaction_date': (datetime.now() - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d'),
                'transaction_time': f"{random.randint(0, 23):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}"
            }
            batch.append(transaction)
        return batch