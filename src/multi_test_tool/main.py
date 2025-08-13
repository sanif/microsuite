"""Main application entry point for multi-test-tool."""

import sys
from dotenv import load_dotenv

from .web_app import main as web_main

def main():
    """Main application entry point - launches web interface."""
    print("🚀 Multi-Test Tool - Web Interface")
    print("Note: Desktop GUI has been removed. Using web interface instead.")
    print("For development with auto-reload, use: python dev.py")
    print("")
    
    try:
        load_dotenv()
        web_main()
        
    except KeyboardInterrupt:
        print("\n🛑 Application interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Error starting application: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()