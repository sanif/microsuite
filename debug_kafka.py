#!/usr/bin/env python3
"""Debug script to test Kafka connection."""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

def test_direct_connection():
    """Test direct Kafka connection."""
    print("Testing direct Kafka connection...")
    print("=" * 50)
    
    # Test with different configurations
    configs = [
        {"bootstrap_servers": ["localhost:9092"]},
        {"bootstrap_servers": ["127.0.0.1:9092"]},
        {"bootstrap_servers": ["localhost:9092"], "api_version": (0, 10, 1)},
        {"bootstrap_servers": ["localhost:9092"], "request_timeout_ms": 30000},
    ]
    
    for i, config in enumerate(configs, 1):
        print(f"\nTest {i}: {config}")
        try:
            # Test admin client
            admin = KafkaAdminClient(**config)
            topics = admin.list_topics()
            print(f"✅ Admin client connected! Found {len(topics)} topics: {list(topics)}")
            admin.close()
            
            # Test producer
            producer_config = config.copy()
            producer_config.update({
                'value_serializer': lambda v: v.encode('utf-8'),
                'request_timeout_ms': 30000,
            })
            producer = KafkaProducer(**producer_config)
            print("✅ Producer client connected!")
            producer.close()
            
            # Test consumer
            consumer_config = config.copy()
            consumer_config.update({
                'auto_offset_reset': 'latest',
                'consumer_timeout_ms': 5000,
                'group_id': 'debug-test-group'
            })
            consumer = KafkaConsumer(**consumer_config)
            print("✅ Consumer client connected!")
            consumer.close()
            
            print("🎉 All connections successful with this config!")
            return config
            
        except NoBrokersAvailable as e:
            print(f"❌ No brokers available: {e}")
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            import traceback
            traceback.print_exc()
    
    return None

def test_kafka_service():
    """Test our Kafka service implementation."""
    print("\n" + "=" * 50)
    print("Testing our Kafka service...")
    print("=" * 50)
    
    try:
        from multi_test_tool.config.settings import Settings
        from multi_test_tool.services.kafka_service import KafkaService
        from multi_test_tool.services.message_handler import MessageHandler
        
        settings = Settings()
        handler = MessageHandler()
        kafka_service = KafkaService(settings, handler)
        
        # Test connection
        success, message = kafka_service.test_connection()
        print(f"Connection test: {success} - {message}")
        
        if success:
            # Test getting topics
            success, topics, message = kafka_service.get_topics()
            print(f"Get topics: {success} - {message}")
            if success:
                print(f"Topics found: {topics}")
        
    except Exception as e:
        print(f"❌ Service test failed: {e}")
        import traceback
        traceback.print_exc()

def check_docker_kafka():
    """Check if Kafka is running in Docker."""
    print("\n" + "=" * 50)
    print("Checking Docker Kafka status...")
    print("=" * 50)
    
    import subprocess
    
    try:
        # Check if Docker is running
        result = subprocess.run(['docker', 'ps'], capture_output=True, text=True)
        if result.returncode != 0:
            print("❌ Docker is not running or not accessible")
            return
        
        # Look for Kafka containers
        lines = result.stdout.split('\n')
        kafka_containers = [line for line in lines if 'kafka' in line.lower()]
        
        if kafka_containers:
            print("✅ Found Kafka containers:")
            for container in kafka_containers:
                print(f"  {container}")
        else:
            print("❌ No Kafka containers found")
            print("Available containers:")
            for line in lines[1:]:  # Skip header
                if line.strip():
                    print(f"  {line}")
        
        # Check port 9092
        print("\nChecking port 9092...")
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex(('localhost', 9092))
        sock.close()
        
        if result == 0:
            print("✅ Port 9092 is open and accepting connections")
        else:
            print("❌ Port 9092 is not accessible")
            
    except Exception as e:
        print(f"❌ Docker check failed: {e}")

def main():
    """Run all diagnostic tests."""
    print("🔍 Kafka Connection Diagnostics")
    print("=" * 50)
    
    check_docker_kafka()
    working_config = test_direct_connection()
    test_kafka_service()
    
    print("\n" + "=" * 50)
    print("🔧 RECOMMENDATIONS:")
    print("=" * 50)
    
    if working_config:
        print("✅ Found working configuration:")
        print(f"   {working_config}")
        print("   Your Kafka service should work with this setup.")
    else:
        print("❌ No working configuration found. Try these steps:")
        print("   1. Verify Kafka is running: docker ps | grep kafka")
        print("   2. Check port mapping: docker port <kafka-container>")
        print("   3. Try connecting to 127.0.0.1:9092 instead of localhost:9092")
        print("   4. Check Kafka logs: docker logs <kafka-container>")
        print("   5. Ensure no firewall is blocking port 9092")

if __name__ == "__main__":
    main()