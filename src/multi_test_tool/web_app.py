"""Web-based interface for multi-test-tool using Flask."""

import os
import json
import threading
from datetime import datetime
from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit
from werkzeug.serving import make_server

from .config.settings import Settings
from .services.kafka_service import KafkaService
from .services.message_handler import MessageHandler
from .utils.helpers import validate_json, export_messages

class WebApp:
    """Web-based Kafka testing application."""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.message_handler = MessageHandler(settings.app_config["max_messages"])
        self.kafka_service = KafkaService(settings, self.message_handler)
        
        self.app = Flask(__name__, template_folder='../templates', static_folder='../static')
        self.app.config['SECRET_KEY'] = 'kafka-test-tool-secret-key'
        self.socketio = SocketIO(self.app, cors_allowed_origins="*")
        
        self.setup_routes()
        self.setup_socketio()
        self.setup_callbacks()
        
        self.server = None
        self.server_thread = None
    
    def setup_routes(self):
        """Setup Flask routes."""
        
        @self.app.route('/')
        def home():
            return render_template('home.html')
        
        @self.app.route('/kafka')
        def kafka():
            return render_template('index.html')
        
        @self.app.route('/api/config')
        def get_config():
            return jsonify({
                'kafka_config': self.settings.kafka_config,
                'app_config': self.settings.app_config
            })
        
        @self.app.route('/api/test-connection', methods=['POST'])
        def test_connection():
            data = request.get_json()
            if data and 'bootstrap_servers' in data:
                servers = data['bootstrap_servers'].split(',')
                self.settings.kafka_config['bootstrap_servers'] = [s.strip() for s in servers]
            
            success, message = self.kafka_service.test_connection()
            return jsonify({'success': success, 'message': message})
        
        @self.app.route('/api/topics')
        def get_topics():
            success, topics, message = self.kafka_service.get_topics()
            return jsonify({
                'success': success,
                'topics': topics,
                'message': message
            })
        
        @self.app.route('/api/send-message', methods=['POST'])
        def send_message():
            data = request.get_json()
            topic = data.get('topic')
            message = data.get('message')
            key = data.get('key') or None
            
            if not topic or not message:
                return jsonify({'success': False, 'message': 'Topic and message are required'})
            
            success, result_message = self.kafka_service.send_message(topic, message, key)
            return jsonify({'success': success, 'message': result_message})
        
        @self.app.route('/api/start-consumer', methods=['POST'])
        def start_consumer():
            data = request.get_json()
            topics = data.get('topics', [])
            group_id = data.get('group_id') or None
            
            if not topics:
                return jsonify({'success': False, 'message': 'Topics are required'})
            
            success, message = self.kafka_service.start_consumer(topics, group_id)
            return jsonify({'success': success, 'message': message})
        
        @self.app.route('/api/stop-consumer', methods=['POST'])
        def stop_consumer():
            success, message = self.kafka_service.stop_consumer()
            return jsonify({'success': success, 'message': message})
        
        @self.app.route('/api/messages')
        def get_messages():
            search = request.args.get('search', '')
            filter_type = request.args.get('filter', 'all')
            limit = int(request.args.get('limit', 100))
            
            if search:
                messages = self.message_handler.search_messages(search)
            else:
                messages = self.message_handler.get_messages()
            
            if filter_type != 'all':
                messages = self.message_handler.filter_by_type(filter_type)
                if search:
                    from .utils.helpers import filter_messages
                    messages = filter_messages(messages, search)
            
            return jsonify({
                'messages': messages[-limit:],
                'statistics': self.message_handler.get_statistics()
            })
        
        @self.app.route('/api/clear-messages', methods=['POST'])
        def clear_messages():
            self.message_handler.clear_messages()
            return jsonify({'success': True, 'message': 'Messages cleared'})
        
        @self.app.route('/api/consumer-status')
        def get_consumer_status():
            return jsonify(self.kafka_service.get_consumer_status())
    
    def setup_socketio(self):
        """Setup SocketIO events."""
        
        @self.socketio.on('connect')
        def handle_connect():
            print('Client connected')
            emit('status', {'message': 'Connected to server'})
        
        @self.socketio.on('disconnect')
        def handle_disconnect():
            print('Client disconnected')
    
    def setup_callbacks(self):
        """Setup service callbacks."""
        self.kafka_service.add_status_callback(self.on_connection_status_change)
        self.message_handler.add_listener(self.on_new_message)
    
    def on_connection_status_change(self, connected: bool, message: str):
        """Handle connection status changes."""
        self.socketio.emit('connection_status', {
            'connected': connected,
            'message': message
        })
    
    def on_new_message(self, message: dict):
        """Handle new messages."""
        self.socketio.emit('new_message', message)
        
        # Emit updated statistics
        stats = self.message_handler.get_statistics()
        self.socketio.emit('statistics_update', stats)
    
    def run(self, host='127.0.0.1', port=5000, debug=False):
        """Run the web application."""
        print(f"Starting Multi-Test Tool web interface...")
        print(f"Access the application at: http://{host}:{port}")
        
        self.socketio.run(
            self.app,
            host=host,
            port=port,
            debug=debug,
            use_reloader=False,
            allow_unsafe_werkzeug=True
        )
    
    def start_background(self, host='127.0.0.1', port=5000):
        """Start the web application in background thread."""
        def run_server():
            self.server = make_server(host, port, self.app)
            self.server.serve_forever()
        
        self.server_thread = threading.Thread(target=run_server, daemon=True)
        self.server_thread.start()
        
        print(f"Multi-Test Tool web interface started at: http://{host}:{port}")
        return f"http://{host}:{port}"
    
    def stop(self):
        """Stop the web application."""
        if self.server:
            self.server.shutdown()
        
        self.kafka_service.cleanup()

def main():
    """Main entry point for web application."""
    from dotenv import load_dotenv
    load_dotenv()
    
    settings = Settings()
    app = WebApp(settings)
    
    try:
        app.run(host='0.0.0.0', port=5000, debug=False)
    except KeyboardInterrupt:
        print("\nShutting down...")
        app.stop()

if __name__ == "__main__":
    main()