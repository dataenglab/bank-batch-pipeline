# test_postgres_creds.py
import psycopg2

def test_credentials():
    credentials = [
        # (dbname, user, password)
        ("postgres", "postgres", "postgres"),
        ("bankdb", "postgres", "postgres"), 
        ("postgres", "admin", "password"),
        ("bankdb", "admin", "password"),
        ("postgres", "postgres", "password"),
        ("bankdb", "postgres", "password"),
    ]
    
    print("Testing PostgreSQL credentials...")
    for dbname, user, password in credentials:
        try:
            conn = psycopg2.connect(
                host="localhost",
                port=5432,
                dbname=dbname,
                user=user,
                password=password
            )
            print(f"SUCCESS: dbname={dbname}, user={user}, password={password}")
            conn.close()
            
            # Update the .env file with working credentials
            with open("../.env", "w") as f:
                f.write(f"DB_HOST=localhost\n")
                f.write(f"DB_PORT=5432\n")
                f.write(f"DB_NAME={dbname}\n")
                f.write(f"DB_USER={user}\n")
                f.write(f"DB_PASSWORD={password}\n")
                f.write(f"S3_ENDPOINT=http://localhost:9000\n")
                f.write(f"S3_ACCESS_KEY=admin\n")
                f.write(f"S3_SECRET_KEY=password\n")
                f.write(f"S3_BUCKET=raw-data\n")
                f.write(f"BATCH_SIZE=1000\n")
                f.write(f"MAX_RETRIES=3\n")
                f.write(f"LOG_LEVEL=INFO\n")
            
            print(f"Updated .env file with working credentials!")
            return True
            
        except Exception as e:
            print(f"FAILED: dbname={dbname}, user={user} - {e}")
    
    print("No working credentials found.")
    return False

if __name__ == "__main__":
    test_credentials()