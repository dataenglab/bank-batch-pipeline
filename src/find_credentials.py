# find_credentials.py
import psycopg2

def test_connection(host, port, dbname, user, password):
    """Test connection with specific credentials"""
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        print(f"SUCCESS: {user}@{host}:{port}/{dbname}")
        conn.close()
        return True
    except Exception as e:
        print(f"FAILED: {user}@{host}:{port}/{dbname} - {e}")
        return False

# Test common credential combinations
common_credentials = [
    # (dbname, user, password)
    ('postgres', 'postgres', 'password'),
    ('postgres', 'postgres', 'postgres'),
    ('bankdb', 'admin', 'password'),
    ('bankdb', 'postgres', 'password'),
    ('postgres', 'admin', 'password'),
    ('bankdb', 'admin', 'admin'),
]

print("Testing PostgreSQL credentials...")
success = False
for dbname, user, password in common_credentials:
    if test_connection('localhost', 5432, dbname, user, password):
        success = True
        print(f"\n Use these credentials in your .env file:")
        print(f"DB_NAME={dbname}")
        print(f"DB_USER={user}") 
        print(f"DB_PASSWORD={password}")
        break

if not success:
    print("\n No working credentials found.")
    print("Try: docker-compose down && docker-compose up -d")