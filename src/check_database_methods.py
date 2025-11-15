#!/usr/bin/env python3
"""
Check what methods are available in DatabaseClient
"""
import sys
import os
sys.path.append(os.path.dirname(__file__))

from database import DatabaseClient

def check_methods():
    print(" DatabaseClient Available Methods:")
    print("=" * 40)
    
    # Create instance
    try:
        db = DatabaseClient()
        print(" DatabaseClient instantiated successfully")
        
        # Get all methods that don't start with _
        methods = [method for method in dir(db) if not method.startswith('_')]
        
        print(f" Found {len(methods)} methods:")
        for method in sorted(methods):
            print(f"   • {method}")
            
        # Check for common transaction methods
        transaction_methods = [m for m in methods if 'transaction' in m.lower() or 'save' in m.lower() or 'insert' in m.lower()]
        
        print(f"\n Transaction-related methods ({len(transaction_methods)}):")
        for method in sorted(transaction_methods):
            print(f"   • {method}")
            
    except Exception as e:
        print(f" Error: {e}")

if __name__ == "__main__":
    check_methods()