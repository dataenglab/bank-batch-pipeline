#!/usr/bin/env python3
"""
Validated Batch Processor - Fixed Version
Works alongside final_working_processor_fixed.py without specific imports
"""
import logging
import json
import sys
import os
import importlib
from datetime import datetime

# Add current directory to path for imports
sys.path.append(os.path.dirname(__file__))

from data_validation import AdvancedDataValidator
from enhanced_data_generator import EnhancedDataGenerator

class ValidatedBatchProcessor:
    """
    Enhanced processor that works with your existing pipeline
    Uses dynamic imports to avoid dependency issues
    """
    
    def __init__(self):
        # Initialize validation components
        self.validator = AdvancedDataValidator()
        self.generator = EnhancedDataGenerator()
        
        # Setup logging
        self.setup_logging()
        self.logger = logging.getLogger(__name__)
        
        # Try to dynamically import existing processor
        self.existing_processor = self._import_existing_processor()
        
        self.logger.info(" ValidatedBatchProcessor initialized successfully")
    
    def _import_existing_processor(self):
        """
        Dynamically import and use the existing processor
        Returns the processor instance or None if not available
        """
        try:
            # Try to import the module
            existing_module = importlib.import_module('final_working_processor_fixed')
            
            # Look for a processor class or function
            processor_instance = None
            
            # Try common class names
            for class_name in ['BatchProcessor', 'DataProcessor', 'BankProcessor']:
                if hasattr(existing_module, class_name):
                    processor_class = getattr(existing_module, class_name)
                    # Try to create instance with common patterns
                    try:
                        processor_instance = processor_class()
                        self.logger.info(f" Found and initialized {class_name}")
                        break
                    except TypeError:
                        # Try with config
                        try:
                            config = self._get_config(existing_module)
                            processor_instance = processor_class(config)
                            self.logger.info(f" Found and initialized {class_name} with config")
                            break
                        except:
                            continue
            
            # If no class found, look for main processing function
            if not processor_instance:
                for func_name in ['process_batch', 'main', 'run_processing']:
                    if hasattr(existing_module, func_name):
                        processor_instance = getattr(existing_module, func_name)
                        self.logger.info(f" Found processing function: {func_name}")
                        break
            
            return processor_instance
            
        except Exception as e:
            self.logger.warning(f" Could not import existing processor: {e}")
            self.logger.info(" Using fallback processing mode")
            return None
    
    def _get_config(self, module):
        """Try to get config from module"""
        try:
            if hasattr(module, 'Config'):
                return module.Config()
        except:
            pass
        return type('Config', (), {'BATCH_SIZE': 100})()
    
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler()]
        )
    
    def validate_transactions(self, transactions):
        """
        Validate a batch of transactions
        Returns: (valid_transactions, validation_report)
        """
        self.logger.info(f" Validating {len(transactions)} transactions...")
        
        valid_transactions = self.validator.validate_batch(transactions)
        validation_report = self.validator.generate_validation_report(transactions)
        
        valid_count = validation_report['valid_transactions']
        total_count = validation_report['total_transactions']
        
        self.logger.info(f" Validation: {valid_count}/{total_count} valid ({valid_count/total_count*100:.1f}%)")
        
        return valid_transactions, validation_report
    
    def process_transactions(self, transactions, use_existing_processor=True):
        """
        Process transactions using either existing processor or fallback
        """
        if use_existing_processor and self.existing_processor:
            self.logger.info(" Using existing processor for processing...")
            
            # Determine how to call the processor
            if callable(self.existing_processor):
                # It's a function
                result = self.existing_processor(transactions)
            else:
                # It's a class instance - try common methods
                for method_name in ['process_batch', 'process', 'run']:
                    if hasattr(self.existing_processor, method_name):
                        method = getattr(self.existing_processor, method_name)
                        result = method(transactions)
                        break
                else:
                    # No common method found, use fallback
                    result = self._fallback_process(transactions)
        else:
            self.logger.info(" Using fallback processing...")
            result = self._fallback_process(transactions)
        
        # Ensure result is a count
        if isinstance(result, int):
            return result
        else:
            # Try to extract count from result
            try:
                if hasattr(result, 'get'):
                    return result.get('processed_count', len(transactions))
                else:
                    return len(transactions)
            except:
                return len(transactions)
    
    def _fallback_process(self, transactions):
        """Fallback processing when existing processor is not available"""
        self.logger.info(f" Fallback processing {len(transactions)} transactions")
        
        # Simulate processing - in real scenario, this would be your database logic
        processed_count = 0
        for transaction in transactions:
            # Here you would normally save to database
            # For demo, we'll just count successful processing
            if transaction.get('amount', 0) > 0:  # Simple validation
                processed_count += 1
        
        self.logger.info(f" Fallback processed {processed_count}/{len(transactions)} transactions")
        return processed_count
    
    def run_validated_pipeline(self, batch_size=10, generate_data=True, input_data=None):
        """
        Run the complete validated pipeline
        """
        self.logger.info(" Starting validated pipeline...")
        
        # Step 1: Get data (generate or use input)
        if generate_data:
            self.logger.info(f" Generating {batch_size} transactions...")
            transactions = self.generator.generate_batch(batch_size)
        else:
            transactions = input_data or []
            self.logger.info(f" Using {len(transactions)} input transactions")
        
        if not transactions:
            self.logger.error(" No transactions to process")
            return {"success": False, "error": "No transactions"}
        
        # Step 2: Validate data
        valid_transactions, validation_report = self.validate_transactions(transactions)
        
        # Step 3: Process only valid data
        if valid_transactions:
            processed_count = self.process_transactions(valid_transactions)
            
            result = {
                "success": True,
                "processed_count": processed_count,
                "original_count": len(transactions),
                "valid_count": len(valid_transactions),
                "rejected_count": len(transactions) - len(valid_transactions),
                "validation_report": validation_report,
                "timestamp": datetime.now().isoformat()
            }
            
            self.logger.info(
                f" Pipeline completed: {processed_count} processed, "
                f"{result['rejected_count']} rejected by validation"
            )
            
        else:
            result = {
                "success": False,
                "processed_count": 0,
                "original_count": len(transactions),
                "valid_count": 0,
                "rejected_count": len(transactions),
                "validation_report": validation_report,
                "error": "All transactions failed validation",
                "timestamp": datetime.now().isoformat()
            }
            
            self.logger.error(" Pipeline failed: No valid transactions")
        
        return result
    
    def demo_comparison(self):
        """
        Demonstrate the value of validation by comparing with/without validation
        """
        print("\n" + "="*60)
        print(" VALIDATED PROCESSOR DEMONSTRATION")
        print("="*60)
        
        # Generate test data with some invalid transactions
        test_data = self.generator.generate_batch(6)
        
        # Add some invalid transactions
        invalid_examples = [
            {'transaction_id': 'INV1', 'amount': -100, 'timestamp': '2024-01-01', 'account_id': 'A1'},
            {'transaction_id': 'INV2', 'amount': 50, 'timestamp': 'invalid_date', 'account_id': 'A2'},
            {'transaction_id': 'INV3', 'amount': 0, 'timestamp': '2024-01-01', 'account_id': 'A3'},
        ]
        test_data.extend(invalid_examples)
        
        print(f"\n Test Data: {len(test_data)} transactions")
        print("   (Includes valid transactions + invalid examples)")
        
        # Run WITH validation
        print(f"\n WITH VALIDATION:")
        print("-" * 30)
        result_with = self.run_validated_pipeline(generate_data=False, input_data=test_data)
        self._print_result(result_with)
        
        # Run WITHOUT validation (simulated)
        print(f"\n WITHOUT VALIDATION:")
        print("-" * 30)
        processed_count = self.process_transactions(test_data)  # Process all, including invalid
        result_without = {
            "processed_count": processed_count,
            "original_count": len(test_data),
            "would_reject": len(test_data) - processed_count,
            "note": "Invalid data would cause processing errors or data corruption"
        }
        
        print(f"    Processed: {result_without['processed_count']}/{result_without['original_count']}")
        print(f"     Would process invalid: {result_without['would_reject']} transactions")
        print(f"    Risk: Data corruption and processing errors")
        
        print(f"\n" + "="*60)
        print(" DEMONSTRATION COMPLETE")
        print("="*60)
        
        return result_with

    def _print_result(self, result):
        """Print formatted result"""
        if result['success']:
            print(f"    SUCCESS")
            print(f"    Original: {result['original_count']} transactions")
            print(f"    Valid: {result['valid_count']} transactions")
            print(f"    Rejected: {result['rejected_count']} transactions")
            print(f"    Processed: {result['processed_count']} transactions")
            
            if result['rejected_count'] > 0:
                print(f"     Protected: {result['rejected_count']} invalid transactions blocked")
        else:
            print(f"    FAILED: {result.get('error', 'Unknown error')}")

def main():
    """Main entry point"""
    try:
        # Setup basic logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        
        print("Starting Validated Batch Processor...")
        
        # Create processor
        processor = ValidatedBatchProcessor()
        
        # Run demonstration
        processor.demo_comparison()
        
        # Also run a small real processing batch
        print(f"\n Running actual processing test...")
        result = processor.run_validated_pipeline(batch_size=5)
        
        if result['success']:
            print(f" Test completed successfully!")
            return 0
        else:
            print(f" Test failed: {result.get('error')}")
            return 1
            
    except Exception as e:
        logging.error(f"Processor failed: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)