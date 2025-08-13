"""Message handling service for multi-test-tool."""

import json
import datetime
from typing import List, Dict, Any, Optional, Callable
from threading import Lock

from ..utils.helpers import format_message, validate_json, filter_messages

class MessageHandler:
    """Handles message processing, storage, and filtering."""
    
    def __init__(self, max_messages: int = 1000):
        self.max_messages = max_messages
        self.messages: List[Dict[str, Any]] = []
        self.lock = Lock()
        self.listeners: List[Callable[[Dict[str, Any]], None]] = []
    
    def add_message(self, content: Any, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Add a message to the handler."""
        timestamp = datetime.datetime.now()
        message = format_message(content, timestamp)
        
        if metadata:
            message.update(metadata)
        
        with self.lock:
            self.messages.append(message)
            
            if len(self.messages) > self.max_messages:
                self.messages = self.messages[-self.max_messages:]
        
        self._notify_listeners(message)
        return message
    
    def add_kafka_message(self, kafka_message, topic: str, partition: int, offset: int) -> Dict[str, Any]:
        """Add a Kafka message with metadata."""
        try:
            content = kafka_message.value.decode('utf-8') if isinstance(kafka_message.value, bytes) else kafka_message.value
            key = kafka_message.key.decode('utf-8') if kafka_message.key and isinstance(kafka_message.key, bytes) else kafka_message.key
        except UnicodeDecodeError:
            content = f"<Binary data: {len(kafka_message.value)} bytes>"
            key = f"<Binary key: {len(kafka_message.key)} bytes>" if kafka_message.key else None
        
        is_json, parsed_content, json_error = validate_json(content) if isinstance(content, str) else (False, None, "Not a string")
        
        metadata = {
            "source": "kafka",
            "topic": topic,
            "partition": partition,
            "offset": offset,
            "key": key,
            "is_json": is_json,
            "json_error": json_error,
            "parsed_content": parsed_content,
            "kafka_timestamp": kafka_message.timestamp,
        }
        
        return self.add_message(content, metadata)
    
    def get_messages(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get messages with optional limit."""
        with self.lock:
            messages = self.messages.copy()
        
        if limit:
            return messages[-limit:]
        return messages
    
    def search_messages(self, search_term: str) -> List[Dict[str, Any]]:
        """Search messages by content."""
        with self.lock:
            messages = self.messages.copy()
        
        return filter_messages(messages, search_term)
    
    def filter_by_topic(self, topic: str) -> List[Dict[str, Any]]:
        """Filter messages by Kafka topic."""
        with self.lock:
            messages = self.messages.copy()
        
        return [msg for msg in messages if msg.get("topic") == topic]
    
    def filter_by_type(self, message_type: str) -> List[Dict[str, Any]]:
        """Filter messages by type (json, string, etc.)."""
        with self.lock:
            messages = self.messages.copy()
        
        if message_type == "json":
            return [msg for msg in messages if msg.get("is_json", False)]
        elif message_type == "text":
            return [msg for msg in messages if not msg.get("is_json", False)]
        
        return messages
    
    def clear_messages(self):
        """Clear all stored messages."""
        with self.lock:
            self.messages.clear()
    
    def add_listener(self, callback: Callable[[Dict[str, Any]], None]):
        """Add a message listener callback."""
        self.listeners.append(callback)
    
    def remove_listener(self, callback: Callable[[Dict[str, Any]], None]):
        """Remove a message listener callback."""
        if callback in self.listeners:
            self.listeners.remove(callback)
    
    def _notify_listeners(self, message: Dict[str, Any]):
        """Notify all listeners of new message."""
        for listener in self.listeners:
            try:
                listener(message)
            except Exception:
                pass
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get message statistics."""
        with self.lock:
            messages = self.messages.copy()
        
        if not messages:
            return {
                "total_messages": 0,
                "json_messages": 0,
                "text_messages": 0,
                "topics": [],
                "time_range": None,
            }
        
        json_count = sum(1 for msg in messages if msg.get("is_json", False))
        topics = list(set(msg.get("topic") for msg in messages if msg.get("topic")))
        
        timestamps = [msg.get("timestamp") for msg in messages if msg.get("timestamp")]
        time_range = None
        if timestamps:
            time_range = {
                "start": min(timestamps),
                "end": max(timestamps),
            }
        
        return {
            "total_messages": len(messages),
            "json_messages": json_count,
            "text_messages": len(messages) - json_count,
            "topics": sorted(topics),
            "time_range": time_range,
        }