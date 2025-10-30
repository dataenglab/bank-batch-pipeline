# src/simple_advanced_demo.py
from data_validator import DataValidator
from error_handler import ErrorHandler
import random
from datetime import datetime

def demo_advanced_features():
    """Simple demo that definitely works"""
    print("=== ADVANCED FEATURES DEMONSTRATION ===")
    
    # 1. Show Data Validation
    print("\n1. DATA VALIDATION DEMO")
    validator = DataValidator()
    
    test_data = [
        # Valid
        {'transaction_id': 'T001', 'account_id': 'ACC123', 'amount': 100.0, 'currency': 'USD', 'timestamp': '2024-01-15 10:30:00'},
        # Invalid
        {'transaction_id': 'T002', 'account_id': '', 'amount': -50.0, 'currency': 'USD', 'timestamp': '2024-01-15 10:30:00'},
    ]
    
    for transaction in test_data:
        is_valid = validator.validate_transaction(transaction)
        status = "VALID" if is_valid else "INVALID"
        print(f"  {transaction['transaction_id']}: {status}")
    
    validation_report = validator.get_validation_report()
    print(f"  Validation success rate: {validation_report['summary']['success_rate']:.2%}")
    
    # 2. Show Error Handling
    print("\n2. ERROR HANDLING DEMO")
    error_handler = ErrorHandler('simple_demo_errors.log')
    
    # Demo retry mechanism
    def demo_operation():
        if not hasattr(demo_operation, 'count'):
            demo_operation.count = 0
        demo_operation.count += 1
        
        if demo_operation.count < 3:
            raise ConnectionError("Simulated network failure")
        return "Operation completed successfully!"
    
    result = error_handler.handle_error(
        'network_error',
        'Network connection failed',
        {'operation': 'demo'},
        demo_operation
    )
    print(f"  Final result: {result}")
    
    # 3. Show Combined Features
    print("\n3. COMBINED FEATURES DEMO")
    def process_with_validation_and_errors(transactions):
        valid_count = 0
        for transaction in transactions:
            # Validate first
            if validator.validate_transaction(transaction):
                # Process with error handling
                def process_op():
                    return f"Processed {transaction['transaction_id']}"
                
                result = error_handler.handle_error(
                    'processing_error',
                    f'Failed to process {transaction["transaction_id"]}',
                    {'transaction': transaction},
                    process_op
                )
                
                if result:
                    valid_count += 1
                    print(f"  ✓ {result}")
        
        return valid_count
    
    # Test with mixed data
    mixed_data = [
        {'transaction_id': 'T003', 'account_id': 'ACC456', 'amount': 200.0, 'currency': 'EUR', 'timestamp': '2024-01-15 11:00:00'},
        {'transaction_id': 'T004', 'account_id': '', 'amount': 300.0, 'currency': 'USD', 'timestamp': '2024-01-15 11:00:00'},  # Invalid
        {'transaction_id': 'T005', 'account_id': 'ACC789', 'amount': 400.0, 'currency': 'GBP', 'timestamp': '2024-01-15 11:00:00'},
    ]
    
    print("  Processing mixed data:")
    valid_count = process_with_validation_and_errors(mixed_data)
    print(f"  Successfully processed: {valid_count}/{len(mixed_data)}")
    
    # 4. Generate Reports
    print("\n4. REPORT GENERATION")
    validator.export_errors('demo_simple_validation.json')
    error_handler.export_error_report('demo_simple_errors.json')
    print("  Reports exported: demo_simple_validation.json, demo_simple_errors.json")
    
    # 5. Show Final Summary
    print("\n5. FINAL SUMMARY")
    val_report = validator.get_validation_report()
    err_report = error_handler.get_error_report()
    
    print(f"  Data Quality: {val_report['summary']['success_rate']:.2%} success rate")
    print(f"  Error Handling: {err_report['summary']['total_errors']} errors handled")
    print(f"  Overall Success: {err_report['success_rate']:.2%} success rate")

if __name__ == '__main__':
    demo_advanced_features()