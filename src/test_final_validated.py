#!/usr/bin/env python3
"""
Simple test for the final validated processor
"""
import sys
import os
sys.path.append(os.path.dirname(__file__))

print("Testing Final Validated Processor...")

try:
    from validated_processor_final import ValidatedBatchProcessor
    print(" Validated processor imported successfully")
    
    # Quick test
    processor = ValidatedBatchProcessor()
    
    # Test data
    test_data = [
        {'transaction_id': 'TEST1', 'amount': 100.0, 'timestamp': '2024-01-01T10:00:00', 'account_id': 'ACC1'},
        {'transaction_id': 'TEST2', 'amount': -50.0, 'timestamp': '2024-01-01T11:00:00', 'account_id': 'ACC2'},  # Invalid
        {'transaction_id': 'TEST3', 'amount': 200.0, 'timestamp': '2024-01-01T12:00:00', 'account_id': 'ACC3'},
    ]
    
    print(f"\n Testing with {len(test_data)} transactions (1 invalid)")
    result = processor.run_validated_pipeline(generate_data=False, input_data=test_data)
    
    print(f" Test completed: {result['success']}")
    print(f" Results: {result['processed_count']} processed, {result['rejected_count']} rejected")
    
    if result['success']:
        print(" All systems working!")
    else:
        print(f" Issue: {result.get('error')}")
        
except Exception as e:
    print(f" Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)