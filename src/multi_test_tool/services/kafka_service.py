"""Kafka service for producer and consumer operations."""

import threading
import time
from typing import List, Dict, Any, Optional, Callable
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError, NoBrokersAvailable, TopicAlreadyExistsError

from ..config.settings import Settings
from .message_handler import MessageHandler

class KafkaService:
    """Kafka producer and consumer service."""
    
    def __init__(self, settings: Settings, message_handler: MessageHandler):
        self.settings = settings
        self.message_handler = message_handler
        self.producer: Optional[KafkaProducer] = None
        self.consumer: Optional[KafkaConsumer] = None
        self.admin_client: Optional[KafkaAdminClient] = None
        self.consumer_thread: Optional[threading.Thread] = None
        self.is_consuming = False
        self.connection_status = False
        self.status_callbacks: List[Callable[[bool, str], None]] = []
    
    def add_status_callback(self, callback: Callable[[bool, str], None]):
        """Add connection status callback."""
        self.status_callbacks.append(callback)
    
    def _notify_status(self, connected: bool, message: str = ""):
        """Notify status callbacks of connection changes."""
        self.connection_status = connected
        for callback in self.status_callbacks:
            try:
                callback(connected, message)
            except Exception:
                pass
    
    def test_connection(self) -> tuple[bool, str]:
        """Test connection to Kafka cluster."""
        try:
            config = self.settings.get_kafka_config()
            test_admin = KafkaAdminClient(**config)
            
            # Try to list topics to test connection
            metadata = test_admin.list_topics()
            topic_count = len(metadata)
            
            test_admin.close()
            
            message = f"Connected successfully! Found {topic_count} topics"
            self._notify_status(True, message)
            return True, message
        
        except NoBrokersAvailable:
            message = "No Kafka brokers available - check bootstrap servers"
            self._notify_status(False, message)
            return False, message
        except Exception as e:
            message = f"Connection failed: {str(e)}"
            self._notify_status(False, message)
            return False, message
    
    def get_topics(self) -> tuple[bool, List[str], str]:
        """Get list of available topics."""
        try:
            if not self.admin_client:
                config = self.settings.get_kafka_config()
                self.admin_client = KafkaAdminClient(**config)
            
            metadata = self.admin_client.list_topics()
            topics = list(metadata)
            
            return True, sorted(topics), "Topics retrieved successfully"
        
        except Exception as e:
            return False, [], f"Failed to get topics: {str(e)}"
    
    def create_topic(self, topic_name: str, num_partitions: int = 1, replication_factor: int = 1) -> tuple[bool, str]:
        """Create a new topic."""
        try:
            if not self.admin_client:
                config = self.settings.get_kafka_config()
                self.admin_client = KafkaAdminClient(**config)
            
            topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor
            )
            
            result = self.admin_client.create_topics([topic])
            
            for topic, future in result.items():
                try:
                    future.result()
                    return True, f"Topic '{topic}' created successfully"
                except TopicAlreadyExistsError:
                    return False, f"Topic '{topic}' already exists"
                except Exception as e:
                    return False, f"Failed to create topic '{topic}': {str(e)}"
        
        except Exception as e:
            return False, f"Failed to create topic: {str(e)}"
    
    def send_message(self, topic: str, message: str, key: Optional[str] = None) -> tuple[bool, str]:
        """Send message to Kafka topic."""
        try:
            if not self.producer:
                config = self.settings.get_producer_config()
                self.producer = KafkaProducer(**config)
            
            future = self.producer.send(
                topic,
                value=message,
                key=key
            )
            
            record_metadata = future.get(timeout=10)
            
            success_msg = f"Message sent to {topic}:{record_metadata.partition}:{record_metadata.offset}"
            return True, success_msg
        
        except Exception as e:
            return False, f"Failed to send message: {str(e)}"
    
    def start_consumer(self, topics: List[str], group_id: str = None) -> tuple[bool, str]:
        """Start consuming messages from topics."""
        if self.is_consuming:
            return False, "Consumer is already running"
        
        try:
            config = self.settings.get_consumer_config(group_id)
            self.consumer = KafkaConsumer(*topics, **config)
            
            self.is_consuming = True
            self.consumer_thread = threading.Thread(
                target=self._consume_messages,
                daemon=True
            )
            self.consumer_thread.start()
            
            return True, f"Started consuming from topics: {', '.join(topics)}"
        
        except Exception as e:
            self.is_consuming = False
            return False, f"Failed to start consumer: {str(e)}"
    
    def stop_consumer(self) -> tuple[bool, str]:
        """Stop consuming messages."""
        if not self.is_consuming:
            return False, "Consumer is not running"
        
        self.is_consuming = False
        
        if self.consumer:
            self.consumer.close()
            self.consumer = None
        
        if self.consumer_thread and self.consumer_thread.is_alive():
            self.consumer_thread.join(timeout=5)
        
        return True, "Consumer stopped"
    
    def _consume_messages(self):
        """Internal method to consume messages in background thread."""
        try:
            while self.is_consuming and self.consumer:
                try:
                    # Poll for messages with timeout
                    message_pack = self.consumer.poll(timeout_ms=1000)
                    
                    if not message_pack:
                        continue
                    
                    for topic_partition, messages in message_pack.items():
                        for message in messages:
                            if not self.is_consuming:
                                return
                            
                            self.message_handler.add_kafka_message(
                                message,
                                message.topic,
                                message.partition,
                                message.offset
                            )
                
                except Exception as e:
                    error_msg = f"Consumer polling error: {str(e)}"
                    print(f"Kafka Consumer Error: {error_msg}")
                    self.message_handler.add_message(
                        error_msg,
                        {"source": "kafka_error", "error_type": "consumer_poll"}
                    )
                    
                    # Continue consuming unless it's a critical error
                    if "No brokers available" in str(e) or "Connection" in str(e):
                        break
                    
                    import time
                    time.sleep(1)  # Brief pause before retrying
        
        except Exception as e:
            error_msg = f"Consumer thread error: {str(e)}"
            print(f"Kafka Consumer Thread Error: {error_msg}")
            self.message_handler.add_message(
                error_msg,
                {"source": "kafka_error", "error_type": "consumer_thread"}
            )
        finally:
            self.is_consuming = False
            print("Consumer thread stopped")
    
    def get_consumer_status(self) -> Dict[str, Any]:
        """Get current consumer status."""
        return {
            "is_consuming": self.is_consuming,
            "consumer_active": self.consumer is not None,
            "thread_alive": self.consumer_thread.is_alive() if self.consumer_thread else False,
        }
    
    def cleanup(self):
        """Cleanup resources."""
        self.stop_consumer()
        
        if self.producer:
            self.producer.close()
            self.producer = None
        
        if self.admin_client:
            self.admin_client.close()
            self.admin_client = None
        
        self._notify_status(False, "Disconnected")