"""Configuration settings for multi-test-tool."""

import os
import time
from typing import Dict, Any

class Settings:
    """Application configuration settings."""
    
    def __init__(self):
        """Initialize settings with default values and environment overrides."""
        self.kafka_config = {
            "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            "security_protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
            "sasl_mechanism": os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
            "sasl_username": os.getenv("KAFKA_SASL_USERNAME", ""),
            "sasl_password": os.getenv("KAFKA_SASL_PASSWORD", ""),
            "ssl_cafile": os.getenv("KAFKA_SSL_CAFILE", ""),
            "ssl_certfile": os.getenv("KAFKA_SSL_CERTFILE", ""),
            "ssl_keyfile": os.getenv("KAFKA_SSL_KEYFILE", ""),
        }
        
        self.web_config = {
            "host": os.getenv("WEB_HOST", "127.0.0.1"),
            "port": int(os.getenv("WEB_PORT", "5000")),
            "debug": os.getenv("WEB_DEBUG", "false").lower() == "true",
        }
        
        self.app_config = {
            "max_messages": int(os.getenv("MAX_MESSAGES", "1000")),
            "consumer_timeout": int(os.getenv("CONSUMER_TIMEOUT", "10")),
            "auto_offset_reset": os.getenv("AUTO_OFFSET_RESET", "latest"),
            "enable_auto_commit": os.getenv("ENABLE_AUTO_COMMIT", "true").lower() == "true",
            "log_level": os.getenv("LOG_LEVEL", "INFO"),
        }
    
    def get_kafka_config(self) -> Dict[str, Any]:
        """Get Kafka configuration."""
        config = {}
        
        for key, value in self.kafka_config.items():
            if value:
                config[key] = value
        
        if not config.get("bootstrap_servers"):
            config["bootstrap_servers"] = ["localhost:9092"]
        elif isinstance(config["bootstrap_servers"], str):
            config["bootstrap_servers"] = config["bootstrap_servers"].split(",")
        
        return config
    
    def get_consumer_config(self, group_id: str = None) -> Dict[str, Any]:
        """Get consumer-specific configuration."""
        config = self.get_kafka_config()
        config.update({
            "auto_offset_reset": self.app_config["auto_offset_reset"],
            "enable_auto_commit": self.app_config["enable_auto_commit"],
            "consumer_timeout_ms": self.app_config["consumer_timeout"] * 1000,
            "session_timeout_ms": 30000,  # 30 seconds
            "heartbeat_interval_ms": 10000,  # 10 seconds
            "max_poll_interval_ms": 300000,  # 5 minutes
            "max_poll_records": 100,  # Limit records per poll
            "fetch_min_bytes": 1,
            "fetch_max_wait_ms": 500,
        })
        
        if group_id:
            config["group_id"] = group_id
        else:
            config["group_id"] = f"multi-test-tool-{int(time.time())}"
        
        return config
    
    def get_producer_config(self) -> Dict[str, Any]:
        """Get producer-specific configuration."""
        config = self.get_kafka_config()
        config.update({
            "value_serializer": lambda v: v.encode('utf-8') if isinstance(v, str) else v,
            "key_serializer": lambda k: k.encode('utf-8') if isinstance(k, str) else k,
        })
        
        return config
    
    def update_kafka_config(self, new_config: Dict[str, Any]):
        """Update Kafka configuration."""
        self.kafka_config.update(new_config)
    
    def update_web_config(self, new_config: Dict[str, Any]):
        """Update web configuration."""
        self.web_config.update(new_config)