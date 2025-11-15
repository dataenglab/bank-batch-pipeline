# test_error_handler.py
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.error_handler import ErrorHandler
import time

def test_error_handler():
    print("=== Testing Advanced Error Handler ===")
    
    error_handler = ErrorHandler('test_errors.log')
    
    # Test 1: Simple error handling
    print("\n1. TESTING BASIC ERROR HANDLING:")
    error_handler.handle_error(
        'data_validation_error',
        'Invalid transaction amount',
        {'transaction_id': 'T001', 'amount': -50}
    )
    
    # Test 2: Retry mechanism for transient errors
    print("\n2. TESTING RETRY MECHANISM:")
    
    def failing_operation():
        # Simulate an operation that fails twice then succeeds
        if not hasattr(failing_operation, 'call_count'):
            failing_operation.call_count = 0
        failing_operation.call_count += 1
        
        if failing_operation.call_count < 3:
            raise ConnectionError("Temporary network failure")
        return "Operation successful"
    
    result = error_handler.handle_error(
        'network_error',
        'Network connection failed',
        {'operation': 'database_query'},
        failing_operation
    )
    print(f"Retry result: {result}")
    
    # Test 3: Critical error (should raise exception)
    print("\n3. TESTING CRITICAL ERROR:")
    try:
        error_handler.handle_error(
            'file_not_found',
            'Required configuration file missing',
            {'file_path': '/config/database.conf'}
        )
    except RuntimeError as e:
        print(f"Critical error handled correctly: {e}")
    
    # Test 4: Generate error report
    print("\n4. ERROR ANALYSIS REPORT:")
    report = error_handler.get_error_report()
    print(f"Total errors: {report['summary']['total_errors']}")
    print(f"Error types: {report['error_breakdown']}")
    print(f"Retry attempts: {report['retry_breakdown']}")
    print(f"Most common error: {report['most_common_error']}")
    print(f"Success rate: {report['success_rate']:.2%}")
    
    # Export report
    error_handler.export_error_report('error_analysis.json')
    print("\nError analysis exported to error_analysis.json")
    
    return error_handler

if __name__ == '__main__':
    test_error_handler()