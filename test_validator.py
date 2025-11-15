# test_validator.py
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.data_validator import DataValidator
import json

def test_validator():
    print("=== Testing Data Validator ===")
    
    # Sample test data (mix of valid and invalid records)
    test_transactions = [
        # Valid transactions
        {
            "transaction_id": "T001",
            "account_id": "ACC123",
            "amount": 100.50,
            "currency": "USD",
            "timestamp": "2024-01-15 10:30:00"
        },
        {
            "transaction_id": "T002", 
            "account_id": "ACC456",
            "amount": 250.75,
            "currency": "EUR",
            "timestamp": "2024-01-15 11:45:00"
        },
        # Invalid transactions
        {
            "transaction_id": "T003",
            "account_id": "",  # Empty account
            "amount": -50.00,  # Negative amount
            "currency": "USD",
            "timestamp": "2024-01-15 12:00:00"
        },
        {
            "transaction_id": "T004",
            "account_id": "ACC789", 
            "amount": 5000000.00,  # Too large amount
            "currency": "XYZ",  # Invalid currency
            "timestamp": "2019-01-01 00:00:00"  # Old timestamp
        },
        {
            "transaction_id": "T005",
            "account_id": "ACC999",
            "amount": 75.25,
            "currency": "USD",
            "timestamp": "invalid_date"  # Invalid timestamp
        }
    ]
    
    validator = DataValidator()
    
    # Test batch validation
    batch_result = validator.validate_batch(test_transactions)
    print("\nBATCH VALIDATION RESULTS:")
    print(f"Total processed: {batch_result['total_processed']}")
    print(f"Valid records: {batch_result['valid_count']}")
    print(f"Invalid records: {batch_result['invalid_count']}")
    print(f"Validity rate: {batch_result['validity_rate']:.2%}")
    
    # Get detailed report
    report = validator.get_validation_report()
    print("\nDETAILED VALIDATION REPORT:")
    print(f"Total checked: {report['summary']['total_checked']}")
    print(f"Errors found: {report['summary']['errors_count']}")
    print(f"Error rate: {report['summary']['error_rate']:.2%}")
    print(f"Success rate: {report['summary']['success_rate']:.2%}")
    
    print("\nERROR BREAKDOWN:")
    for error_type, count in report['error_breakdown'].items():
        print(f"  {error_type}: {count} errors")
    
    # Export errors to file
    validator.export_errors('validation_errors.json')
    print(f"\nErrors exported to: validation_errors.json")
    
    # Show sample of what was exported (with error handling)
    print("\nSAMPLE OF EXPORTED ERRORS:")
    with open('validation_errors.json', 'r') as f:
        exported_data = json.load(f)
        sample_errors = exported_data['validation_report'].get('sample_errors', [])
        
        if sample_errors:
            for i, error in enumerate(sample_errors[:2]):
                print(f"Error {i+1}: {error['failed_checks']}")
        else:
            print("No errors to display")
    
    return validator

if __name__ == '__main__':
    test_validator()