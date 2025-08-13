#!/usr/bin/env python3
"""Simple startup script for the web version of Multi-Test Tool."""

import sys
import os
import webbrowser
import time

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from multi_test_tool.web_app import WebApp
from multi_test_tool.config.settings import Settings

def main():
    """Start the web application."""
    try:
        settings = Settings()
        app = WebApp(settings)
        
        port = 5001  # Use 5001 to avoid conflicts with macOS AirPlay
        host = '127.0.0.1'
        url = f"http://{host}:{port}"
        
        print("=" * 60)
        print("🚀 Multi-Test Tool - Web Interface")
        print("=" * 60)
        print(f"Starting server on: {url}")
        print("Features:")
        print("  ✅ Kafka Producer/Consumer Testing")
        print("  ✅ Topic Management")
        print("  ✅ Real-time Message Display")
        print("  ✅ Search & Filter Messages")
        print("  ✅ Export Functionality")
        print("=" * 60)
        print("Press Ctrl+C to stop the server")
        print()
        
        # Try to open browser automatically
        try:
            webbrowser.open(url)
            print(f"🌐 Opening browser to {url}")
        except:
            print(f"💡 Open your browser to: {url}")
        
        print()
        
        app.run(host=host, port=port, debug=False)
        
    except KeyboardInterrupt:
        print("\n\n👋 Shutting down Multi-Test Tool...")
        print("Thank you for using Multi-Test Tool!")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Error starting application: {e}")
        print("\n💡 Troubleshooting:")
        print("  1. Make sure port 5001 is available")
        print("  2. Check your Python environment")
        print("  3. Verify all dependencies are installed")
        sys.exit(1)

if __name__ == "__main__":
    main()