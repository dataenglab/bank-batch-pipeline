from prometheus_client import start_http_server
import time
import signal
import sys

print("=== Starting Test Metrics Server ===")

# Start the server
start_http_server(8000)
print("✓ Server started on port 8000")
print("✓ Metrics available at: http://localhost:8000/metrics")
print("✓ Server is running in the background...")
print("Press Ctrl+C to stop the server")

# Set up signal handler for graceful shutdown
def signal_handler(sig, frame):
    print("\nServer stopped by user")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# Keep the server running
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Server stopped")