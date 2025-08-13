"""Helper utilities for multi-test-tool."""

import json
import csv
import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path

def format_message(message: Any, timestamp: Optional[datetime.datetime] = None) -> Dict[str, Any]:
    """Format a message with timestamp and metadata."""
    if timestamp is None:
        timestamp = datetime.datetime.now()
    
    return {
        "timestamp": timestamp.isoformat(),
        "content": message,
        "type": type(message).__name__,
        "size": len(str(message)) if message else 0
    }

def validate_json(text: str) -> tuple[bool, Optional[Dict], Optional[str]]:
    """Validate if text is valid JSON."""
    try:
        parsed = json.loads(text)
        return True, parsed, None
    except json.JSONDecodeError as e:
        return False, None, str(e)

def pretty_format_json(obj: Any) -> str:
    """Format object as pretty-printed JSON."""
    try:
        return json.dumps(obj, indent=2, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        return str(obj)

def export_messages(messages: List[Dict[str, Any]], file_path: str, format_type: str = "json") -> bool:
    """Export messages to file in specified format."""
    try:
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        if format_type.lower() == "json":
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(messages, f, indent=2, ensure_ascii=False, default=str)
        
        elif format_type.lower() == "csv":
            if messages:
                with open(path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=messages[0].keys())
                    writer.writeheader()
                    for msg in messages:
                        flattened = flatten_dict(msg)
                        writer.writerow(flattened)
        
        elif format_type.lower() == "txt":
            with open(path, 'w', encoding='utf-8') as f:
                for msg in messages:
                    f.write(f"[{msg.get('timestamp', 'N/A')}] {msg.get('content', '')}\n")
        
        else:
            return False
        
        return True
    
    except Exception:
        return False

def flatten_dict(d: Dict[str, Any], parent_key: str = '', sep: str = '.') -> Dict[str, Any]:
    """Flatten nested dictionary for CSV export."""
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            items.append((new_key, json.dumps(v)))
        else:
            items.append((new_key, v))
    return dict(items)

def truncate_text(text: str, max_length: int = 100) -> str:
    """Truncate text to specified length with ellipsis."""
    if len(text) <= max_length:
        return text
    return text[:max_length-3] + "..."

def parse_kafka_bootstrap_servers(servers_str: str) -> List[str]:
    """Parse bootstrap servers string into list."""
    if not servers_str:
        return ["localhost:9092"]
    
    servers = [s.strip() for s in servers_str.split(",")]
    return [s for s in servers if s]

def get_timestamp_string(dt: Optional[datetime.datetime] = None) -> str:
    """Get formatted timestamp string."""
    if dt is None:
        dt = datetime.datetime.now()
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

def filter_messages(messages: List[Dict[str, Any]], search_term: str) -> List[Dict[str, Any]]:
    """Filter messages by search term."""
    if not search_term:
        return messages
    
    search_lower = search_term.lower()
    filtered = []
    
    for msg in messages:
        content = str(msg.get('content', '')).lower()
        timestamp = str(msg.get('timestamp', '')).lower()
        
        if search_lower in content or search_lower in timestamp:
            filtered.append(msg)
    
    return filtered