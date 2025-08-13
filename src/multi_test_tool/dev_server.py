#!/usr/bin/env python3
"""Development server with auto-reload functionality using watchdog."""

import os
import sys
import time
import threading
import subprocess
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from .config.settings import Settings
from .web_app import WebApp


class ReloadHandler(FileSystemEventHandler):
    """File system event handler for auto-reloading."""

    def __init__(self, restart_callback, watched_extensions=None):
        super().__init__()
        self.restart_callback = restart_callback
        self.watched_extensions = watched_extensions or ['.py', '.html', '.css', '.js']
        self.last_restart = 0
        self.restart_delay = 1.0  # Delay in seconds to prevent rapid restarts

    def on_modified(self, event):
        """Handle file modification events."""
        if event.is_directory:
            return

        file_path = Path(event.src_path)
        if file_path.suffix.lower() in self.watched_extensions:
            current_time = time.time()
            if current_time - self.last_restart > self.restart_delay:
                print(f"\n📝 File changed: {file_path.name}")
                self.last_restart = current_time
                threading.Thread(target=self.restart_callback, daemon=True).start()


class DevServer:
    """Development server with file watching and auto-reload capabilities."""

    def __init__(self, host='127.0.0.1', port=5000):
        self.host = host
        self.port = port
        self.app = None
        self.observer = None
        self.running = False
        self.restart_requested = False

    def start_app(self):
        """Start the Flask application."""
        try:
            settings = Settings()
            self.app = WebApp(settings)
            
            print(f"🚀 Starting Multi-Test Tool development server...")
            print(f"📍 Server: http://{self.host}:{self.port}")
            print(f"👀 Watching for changes in Python, HTML, CSS, and JS files")
            print(f"🔄 Auto-reload enabled - modify files to trigger restart")
            print(f"⏹️  Press Ctrl+C to stop\n")
            
            # Run the app (this will block)
            self.app.socketio.run(
                self.app.app,
                host=self.host,
                port=self.port,
                debug=False,  # We handle reloading ourselves
                use_reloader=False,  # Disable Flask's built-in reloader
                allow_unsafe_werkzeug=True
            )
        except Exception as e:
            print(f"❌ Error starting server: {e}")

    def setup_file_watcher(self):
        """Set up file system watcher."""
        # Get the source directory
        src_dir = Path(__file__).parent.parent
        
        # Create event handler and observer
        handler = ReloadHandler(self.request_restart)
        self.observer = Observer()
        
        # Watch the source directory recursively
        self.observer.schedule(handler, str(src_dir), recursive=True)
        
        # Also watch templates and static files
        templates_dir = src_dir / "templates"
        static_dir = src_dir / "static"
        
        if templates_dir.exists():
            self.observer.schedule(handler, str(templates_dir), recursive=True)
        if static_dir.exists():
            self.observer.schedule(handler, str(static_dir), recursive=True)
        
        self.observer.start()
        print(f"👁️  Watching directories:")
        print(f"   • {src_dir}")
        if templates_dir.exists():
            print(f"   • {templates_dir}")
        if static_dir.exists():
            print(f"   • {static_dir}")

    def request_restart(self):
        """Request a server restart."""
        if self.running and not self.restart_requested:
            self.restart_requested = True
            print("🔄 Restarting server due to file changes...")
            
            # Stop the current app
            if self.app and hasattr(self.app, 'socketio'):
                try:
                    self.app.stop()
                except:
                    pass
            
            # Small delay to allow cleanup
            time.sleep(0.5)
            
            # Restart using subprocess (this avoids import caching issues)
            self.restart_process()

    def restart_process(self):
        """Restart the entire process."""
        try:
            # Get the current script arguments
            args = [sys.executable] + sys.argv
            
            # Stop the file watcher
            if self.observer:
                self.observer.stop()
            
            print("🔄 Restarting process...")
            
            # Replace the current process
            if sys.platform == 'win32':
                # On Windows, use subprocess
                subprocess.Popen(args)
                sys.exit(0)
            else:
                # On Unix-like systems, use execv for clean restart
                os.execv(sys.executable, args)
                
        except Exception as e:
            print(f"❌ Error during restart: {e}")
            sys.exit(1)

    def run(self):
        """Run the development server with file watching."""
        self.running = True
        
        try:
            # Set up file watching
            self.setup_file_watcher()
            
            # Start the Flask app (this will block until stopped)
            self.start_app()
            
        except KeyboardInterrupt:
            print("\n🛑 Shutting down development server...")
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
        finally:
            self.cleanup()

    def cleanup(self):
        """Clean up resources."""
        self.running = False
        
        if self.observer:
            self.observer.stop()
            self.observer.join()
        
        if self.app:
            try:
                self.app.stop()
            except:
                pass
        
        print("✅ Development server stopped.")


def main():
    """Main entry point for the development server."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Multi-Test Tool Development Server with Auto-reload"
    )
    parser.add_argument(
        '--host', 
        default='127.0.0.1',
        help='Host to bind to (default: 127.0.0.1)'
    )
    parser.add_argument(
        '--port', 
        type=int, 
        default=5000,
        help='Port to bind to (default: 5000)'
    )
    
    args = parser.parse_args()
    
    # Create and run development server
    dev_server = DevServer(host=args.host, port=args.port)
    dev_server.run()


if __name__ == "__main__":
    main()