#!/usr/bin/env python3
"""Test script to debug consumer issues."""

import sys
import os
import time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from multi_test_tool.config.settings import Settings
from multi_test_tool.services.kafka_service import KafkaService
from multi_test_tool.services.message_handler import MessageHandler

def test_consumer():
    """Test consumer functionality."""
    print("🔍 Testing Consumer Stability")
    print("=" * 50)
    
    settings = Settings()
    handler = MessageHandler()
    kafka_service = KafkaService(settings, handler)
    
    # Test connection first
    success, message = kafka_service.test_connection()
    print(f"Connection: {'✅' if success else '❌'} {message}")
    
    if not success:
        return
    
    # Get topics
    success, topics, message = kafka_service.get_topics()
    print(f"Topics: {'✅' if success else '❌'} {message}")
    if topics:
        print(f"Available topics: {topics}")
        
        # Test with first available topic that's not __consumer_offsets
        test_topics = [t for t in topics if not t.startswith('__')]
        if test_topics:
            test_topic = test_topics[0]
            print(f"\nTesting consumer with topic: {test_topic}")
            
            # Start consumer
            success, message = kafka_service.start_consumer([test_topic], "test-consumer-group")
            print(f"Start consumer: {'✅' if success else '❌'} {message}")
            
            if success:
                print("Consumer started successfully!")
                print("Monitoring for 30 seconds...")
                
                for i in range(30):
                    time.sleep(1)
                    status = kafka_service.get_consumer_status()
                    messages = handler.get_messages()
                    
                    print(f"\r[{i+1:2d}s] Consumer: {'🟢' if status['is_consuming'] else '🔴'} | Messages: {len(messages)}", end='', flush=True)
                    
                    if not status['is_consuming']:
                        print(f"\n❌ Consumer stopped unexpectedly at {i+1}s")
                        break
                
                print("\n\nStopping consumer...")
                success, message = kafka_service.stop_consumer()
                print(f"Stop consumer: {'✅' if success else '❌'} {message}")
                
                # Show final message count
                final_messages = handler.get_messages()
                print(f"Final message count: {len(final_messages)}")
                
                if final_messages:
                    print("\nLast few messages:")
                    for msg in final_messages[-3:]:
                        print(f"  - {msg.get('timestamp', 'N/A')}: {str(msg.get('content', ''))[:50]}...")

def main():
    """Run consumer test."""
    try:
        test_consumer()
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()