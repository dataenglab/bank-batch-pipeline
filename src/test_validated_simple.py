#!/usr/bin/env python3
"""
Simple test for validated processor
"""
import sys
import os
sys.path.append(os.path.dirname(__file__))

try:
    from validated_processor_fixed import ValidatedBatchProcessor
    print(" Validated processor imported successfully")
    
    # Simple test
    processor = ValidatedBatchProcessor()
    
    # Test with minimal data
    test_data = [
        {'transaction_id': 'TEST1', 'amount': 100.0, 'timestamp': '2024-01-01T10:00:00', 'account_id': 'ACC1'},
        {'transaction_id': 'TEST2', 'amount': -50.0, 'timestamp': '2024-01-01T11:00:00', 'account_id': 'ACC2'},  # Invalid
    ]
    
    result = processor.run_validated_pipeline(generate_data=False, input_data=test_data)
    print(f" Test completed: {result['success']}")
    print(f" Processed: {result['processed_count']}, Rejected: {result['rejected_count']}")
    
except Exception as e:
    print(f" Error: {e}")
    sys.exit(1)