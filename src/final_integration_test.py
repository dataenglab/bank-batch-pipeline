#!/usr/bin/env python3
"""
Final integration test - validates the complete pipeline works
"""
import sys
import os
sys.path.append(os.path.dirname(__file__))

print(" FINAL INTEGRATION TEST - VALIDATED PIPELINE")
print("=" * 50)

try:
    from validated_processor_database import ValidatedBatchProcessor
    print(" Validated processor imported")
    
    from data_validation import AdvancedDataValidator
    print(" Data validator imported")
    
    from enhanced_data_generator import EnhancedDataGenerator
    print(" Data generator imported")
    
    # Test all components
    processor = ValidatedBatchProcessor()
    validator = AdvancedDataValidator()
    generator = EnhancedDataGenerator()
    
    print(" All components initialized")
    
    # Generate test data
    test_data = generator.generate_batch(5)
    print(f" Generated {len(test_data)} test transactions")
    
    # Validate data
    valid_data = validator.validate_batch(test_data)
    print(f" Validation: {len(valid_data)}/{len(test_data)} valid")
    
    # Run full pipeline
    result = processor.run_production_pipeline(batch_size=10)
    
    print(f"\n FINAL RESULT: {'SUCCESS' if result['success'] else 'FAILED'}")
    print(f" Processed: {result['processed_count']} transactions")
    print(f"  Protected: {result['rejected_count']} invalid transactions blocked")
    print(f" Database: {'Used' if result.get('database_used') else 'Fallback'}")
    
    if result['success']:
        print("\n INTEGRATION TEST PASSED - SYSTEM READY FOR PRODUCTION")
        sys.exit(0)
    else:
        print("\n INTEGRATION TEST FAILED")
        sys.exit(1)
        
except Exception as e:
    print(f" INTEGRATION TEST FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)