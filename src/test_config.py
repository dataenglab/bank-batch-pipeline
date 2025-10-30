# test_config.py
from config import config

def test_configuration():
    print("=== Testing configuration ===")
    
    # Test that config object exists
    assert config is not None, "Config object should not be None"
    
    # Test basic attributes
    assert hasattr(config, 'DB_HOST'), "Config should have DB_HOST"
    assert hasattr(config, 'S3_BUCKET'), "Config should have S3_BUCKET"
    assert hasattr(config, 'BATCH_SIZE'), "Config should have BATCH_SIZE"
    
    # Test specific values
    assert config.S3_BUCKET == 'raw-data', f"S3_BUCKET should be 'raw-data', got {config.S3_BUCKET}"
    assert config.BATCH_SIZE > 0, f"BATCH_SIZE should be positive, got {config.BATCH_SIZE}"
    
    print("Basic configuration test passed!")
    print(f"DB_HOST: {config.DB_HOST}")
    print(f"S3_BUCKET: {config.S3_BUCKET}")
    print(f"BATCH_SIZE: {config.BATCH_SIZE}")
    print("All configuration tests passed!")

if __name__ == "__main__":
    test_configuration()