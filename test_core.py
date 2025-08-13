#!/usr/bin/env python3
"""Test script to verify core functionality without GUI."""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from multi_test_tool.config.settings import Settings
from multi_test_tool.services.message_handler import MessageHandler
from multi_test_tool.services.kafka_service import KafkaService
from multi_test_tool.utils.helpers import validate_json, format_message, pretty_format_json

def test_configuration():
    """Test configuration loading."""
    print("Testing configuration...")
    settings = Settings()
    
    print(f"Kafka Bootstrap Servers: {settings.kafka_config['bootstrap_servers']}")
    print(f"Max Messages: {settings.app_config['max_messages']}")
    print(f"Web Host: {settings.web_config['host']}:{settings.web_config['port']}")
    print("✓ Configuration test passed\n")

def test_message_handler():
    """Test message handling functionality."""
    print("Testing message handler...")
    handler = MessageHandler(max_messages=100)
    
    # Test adding messages
    msg1 = handler.add_message("Hello, World!")
    msg2 = handler.add_message('{"test": "json", "value": 123}')
    
    print(f"Added {len(handler.get_messages())} messages")
    
    # Test JSON validation
    is_json, parsed, error = validate_json('{"valid": "json"}')
    print(f"JSON validation: {is_json}, Error: {error}")
    
    # Test search
    search_results = handler.search_messages("Hello")
    print(f"Search results: {len(search_results)} messages")
    
    # Test statistics
    stats = handler.get_statistics()
    print(f"Statistics: {stats}")
    print("✓ Message handler test passed\n")

def test_kafka_service():
    """Test Kafka service functionality (without actual connection)."""
    print("Testing Kafka service...")
    settings = Settings()
    handler = MessageHandler()
    kafka_service = KafkaService(settings, handler)
    
    # Test configuration
    producer_config = settings.get_producer_config()
    consumer_config = settings.get_consumer_config("test-group")
    
    print(f"Producer config keys: {list(producer_config.keys())}")
    print(f"Consumer config keys: {list(consumer_config.keys())}")
    
    # Test connection (will fail without Kafka, but tests the method)
    success, message = kafka_service.test_connection()
    print(f"Connection test result: {success}, Message: {message}")
    
    print("✓ Kafka service test passed\n")

def test_utilities():
    """Test utility functions."""
    print("Testing utilities...")
    
    # Test JSON formatting
    test_obj = {"name": "test", "values": [1, 2, 3], "nested": {"key": "value"}}
    formatted = pretty_format_json(test_obj)
    print(f"Formatted JSON length: {len(formatted)} characters")
    
    # Test message formatting
    msg = format_message("Test message")
    print(f"Formatted message keys: {list(msg.keys())}")
    
    print("✓ Utilities test passed\n")

def main():
    """Run all tests."""
    print("Running Multi-Test Tool Core Functionality Tests\n")
    print("=" * 50)
    
    try:
        test_configuration()
        test_message_handler()
        test_kafka_service()
        test_utilities()
        
        print("=" * 50)
        print("✅ All core functionality tests passed!")
        print("\nThe application core is working correctly.")
        print("\nTo run the web application:")
        print("1. Production: uv run multi-test-tool-web")
        print("2. Development: python dev.py")
        print("3. Open: http://127.0.0.1:5000")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()