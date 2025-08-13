// Multi-Test Tool JavaScript Application

class KafkaTestTool {
    constructor() {
        this.socket = io();
        this.messages = [];
        this.topics = [];
        this.isConsumerRunning = false;
        
        this.init();
    }
    
    init() {
        this.setupSocketListeners();
        this.setupEventListeners();
        this.loadConfiguration();
        this.updateUI();
    }
    
    setupSocketListeners() {
        this.socket.on('connect', () => {
            console.log('Connected to server');
            this.showToast('Connected to server', 'success');
        });
        
        this.socket.on('disconnect', () => {
            console.log('Disconnected from server');
            this.showToast('Disconnected from server', 'error');
        });
        
        this.socket.on('connection_status', (data) => {
            this.updateConnectionStatus(data.connected, data.message);
        });
        
        this.socket.on('new_message', (message) => {
            this.addMessage(message);
        });
        
        this.socket.on('statistics_update', (stats) => {
            this.updateStatistics(stats);
        });
    }
    
    setupEventListeners() {
        // Search and filter
        document.getElementById('search-messages').addEventListener('input', () => {
            this.filterMessages();
        });
        
        document.querySelectorAll('input[name="filter"]').forEach(radio => {
            radio.addEventListener('change', () => {
                this.filterMessages();
            });
        });
        
        // Auto-refresh consumer status
        setInterval(() => {
            if (this.isConsumerRunning) {
                this.checkConsumerStatus();
            }
        }, 5000);
    }
    
    async loadConfiguration() {
        try {
            const response = await fetch('/api/config');
            const config = await response.json();
            
            if (config.kafka_config.bootstrap_servers) {
                document.getElementById('bootstrap-servers').value = 
                    Array.isArray(config.kafka_config.bootstrap_servers) 
                        ? config.kafka_config.bootstrap_servers.join(',')
                        : config.kafka_config.bootstrap_servers;
            }
        } catch (error) {
            console.error('Failed to load configuration:', error);
        }
    }
    
    async testConnection() {
        const servers = document.getElementById('bootstrap-servers').value;
        const button = event.target;
        
        button.disabled = true;
        button.innerHTML = '<div class=\"spinner\"></div>Testing...';
        
        try {
            const response = await fetch('/api/test-connection', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ bootstrap_servers: servers })
            });
            
            const result = await response.json();
            this.showToast(result.message, result.success ? 'success' : 'error');
            
            if (result.success) {
                this.refreshTopics();
            }
        } catch (error) {
            this.showToast('Connection test failed: ' + error.message, 'error');
        } finally {
            button.disabled = false;
            button.innerHTML = '<i class=\"fas fa-plug\"></i> Test Connection';
        }
    }
    
    async refreshTopics() {
        const button = event.target;
        button.disabled = true;
        button.innerHTML = '<div class=\"spinner\"></div>Loading...';
        
        try {
            const response = await fetch('/api/topics');
            const result = await response.json();
            
            if (result.success) {
                this.topics = result.topics;
                this.updateTopicsDropdown();
                this.showToast(`Loaded ${result.topics.length} topics`, 'success');
            } else {
                this.showToast(result.message, 'error');
            }
        } catch (error) {
            this.showToast('Failed to load topics: ' + error.message, 'error');
        } finally {
            button.disabled = false;
            button.innerHTML = '<i class=\"fas fa-refresh\"></i> Refresh Topics';
        }
    }
    
    updateTopicsDropdown() {
        // Update producer topic dropdown
        const producerSelect = document.getElementById('producer-topic');
        producerSelect.innerHTML = '<option value=\"\">Select a topic...</option>';
        
        this.topics.forEach(topic => {
            const option = document.createElement('option');
            option.value = topic;
            option.textContent = topic;
            producerSelect.appendChild(option);
        });
        
        // Update consumer topics multi-select
        const consumerSelect = document.getElementById('consumer-topics');
        consumerSelect.innerHTML = '';
        
        if (this.topics.length === 0) {
            const option = document.createElement('option');
            option.value = '';
            option.disabled = true;
            option.textContent = 'No topics available - refresh topics first';
            consumerSelect.appendChild(option);
        } else {
            this.topics.forEach(topic => {
                const option = document.createElement('option');
                option.value = topic;
                option.textContent = topic;
                consumerSelect.appendChild(option);
            });
        }
    }
    
    async sendMessage() {
        const topic = document.getElementById('producer-topic').value;
        const message = document.getElementById('message-content').value;
        const key = document.getElementById('message-key').value;
        
        if (!topic) {
            this.showToast('Please select a target topic', 'error');
            return;
        }
        
        if (!message) {
            this.showToast('Message content is required', 'error');
            return;
        }
        
        const button = event.target;
        button.disabled = true;
        button.innerHTML = '<div class=\"spinner\"></div>Sending...';
        
        try {
            const response = await fetch('/api/send-message', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ topic, message, key })
            });
            
            const result = await response.json();
            this.showToast(result.message, result.success ? 'success' : 'error');
            
            if (result.success) {
                document.getElementById('message-content').value = '';
                document.getElementById('message-key').value = '';
            }
        } catch (error) {
            this.showToast('Failed to send message: ' + error.message, 'error');
        } finally {
            button.disabled = false;
            button.innerHTML = '<i class=\"fas fa-send\"></i> Send Message';
        }
    }
    
    async startConsumer() {
        const topicsSelect = document.getElementById('consumer-topics');
        const groupId = document.getElementById('consumer-group').value;
        
        // Get selected topics from multi-select
        const selectedOptions = Array.from(topicsSelect.selectedOptions);
        const topics = selectedOptions.map(option => option.value).filter(t => t);
        
        if (topics.length === 0) {
            this.showToast('Please select at least one topic', 'error');
            return;
        }
        
        try {
            const response = await fetch('/api/start-consumer', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ topics, group_id: groupId })
            });
            
            const result = await response.json();
            this.showToast(result.message, result.success ? 'success' : 'error');
            
            if (result.success) {
                this.isConsumerRunning = true;
                this.updateConsumerUI();
            }
        } catch (error) {
            this.showToast('Failed to start consumer: ' + error.message, 'error');
        }
    }
    
    async stopConsumer() {
        try {
            const response = await fetch('/api/stop-consumer', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' }
            });
            
            const result = await response.json();
            this.showToast(result.message, result.success ? 'success' : 'error');
            
            if (result.success) {
                this.isConsumerRunning = false;
                this.updateConsumerUI();
            }
        } catch (error) {
            this.showToast('Failed to stop consumer: ' + error.message, 'error');
        }
    }
    
    async checkConsumerStatus() {
        try {
            const response = await fetch('/api/consumer-status');
            const status = await response.json();
            
            if (!status.is_consuming && this.isConsumerRunning) {
                this.isConsumerRunning = false;
                this.updateConsumerUI();
                this.showToast('Consumer stopped unexpectedly', 'warning');
            }
        } catch (error) {
            console.error('Failed to check consumer status:', error);
        }
    }
    
    updateConsumerUI() {
        const startBtn = document.getElementById('start-consumer-btn');
        const stopBtn = document.getElementById('stop-consumer-btn');
        
        startBtn.disabled = this.isConsumerRunning;
        stopBtn.disabled = !this.isConsumerRunning;
    }
    
    async filterMessages() {
        const search = document.getElementById('search-messages').value;
        const filter = document.querySelector('input[name=\"filter\"]:checked').value;
        
        try {
            const response = await fetch(`/api/messages?search=${encodeURIComponent(search)}&filter=${filter}&limit=100`);
            const result = await response.json();
            
            this.displayMessages(result.messages);
            this.updateStatistics(result.statistics);
        } catch (error) {
            console.error('Failed to filter messages:', error);
        }
    }
    
    addMessage(message) {
        this.messages.unshift(message);
        if (this.messages.length > 1000) {
            this.messages = this.messages.slice(0, 1000);
        }
        
        // Check if message matches current filter
        const search = document.getElementById('search-messages').value.toLowerCase();
        const filter = document.querySelector('input[name=\"filter\"]:checked').value;
        
        let shouldShow = true;
        
        if (search && !JSON.stringify(message).toLowerCase().includes(search)) {
            shouldShow = false;
        }
        
        if (filter === 'json' && !message.is_json) {
            shouldShow = false;
        } else if (filter === 'text' && message.is_json) {
            shouldShow = false;
        }
        
        if (shouldShow) {
            this.prependMessageToDisplay(message);
        }
    }
    
    displayMessages(messages) {
        const container = document.getElementById('messages-container');
        
        if (messages.length === 0) {
            container.innerHTML = `
                <div class="empty-state">
                    <div class="empty-icon">
                        <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
                            <circle cx="11" cy="11" r="8"/>
                            <path d="m21 21-4.35-4.35"/>
                        </svg>
                    </div>
                    <h3>No matches found</h3>
                    <p>Try adjusting your search or filter criteria</p>
                </div>
            `;
            return;
        }
        
        container.innerHTML = '';
        messages.reverse().forEach(message => {
            this.appendMessageToDisplay(message);
        });
        
        container.scrollTop = container.scrollHeight;
    }
    
    prependMessageToDisplay(message) {
        const container = document.getElementById('messages-container');
        
        // Remove placeholder if it exists
        const placeholder = container.querySelector('.text-center');
        if (placeholder) {
            placeholder.remove();
        }
        
        const messageEl = this.createMessageElement(message);
        messageEl.classList.add('new');
        container.insertBefore(messageEl, container.firstChild);
        
        // Auto-scroll if user is at the bottom
        if (container.scrollTop + container.clientHeight >= container.scrollHeight - 100) {
            container.scrollTop = container.scrollHeight;
        }
    }
    
    appendMessageToDisplay(message) {
        const container = document.getElementById('messages-container');
        const messageEl = this.createMessageElement(message);
        container.appendChild(messageEl);
    }
    
    createMessageElement(message) {
        const div = document.createElement('div');
        div.className = 'message-item';
        
        const timestamp = new Date(message.timestamp).toLocaleString();
        const content = message.content || '';
        const isJson = message.is_json || false;
        
        div.innerHTML = `
            <div class=\"message-header\">
                <span><i class=\"fas fa-clock\"></i> ${timestamp}</span>
                <div class=\"message-badges\">
                    ${message.topic ? `<span class=\"badge badge-topic\">${message.topic}</span>` : ''}
                    ${message.key ? `<span class=\"badge badge-key\">Key: ${message.key}</span>` : ''}
                    ${isJson ? '<span class=\"badge badge-json\">JSON</span>' : ''}
                </div>
            </div>
            <div class=\"message-content ${isJson ? 'json' : ''}\">${this.escapeHtml(content)}</div>
            <div class=\"message-meta\">
                ${message.partition !== undefined ? `<span>Partition: ${message.partition}</span>` : ''}
                ${message.offset !== undefined ? `<span>Offset: ${message.offset}</span>` : ''}
                <span>Size: ${message.size || 0} bytes</span>
            </div>
        `;
        
        return div;
    }
    
    updateConnectionStatus(connected, message) {
        const indicator = document.getElementById('connection-indicator');
        const text = document.getElementById('connection-text');
        
        if (connected) {
            indicator.className = 'connection-indicator connected';
            text.textContent = 'Connected';
        } else {
            indicator.className = 'connection-indicator disconnected';
            text.textContent = 'Disconnected';
        }
        
        if (message) {
            this.showToast(message, connected ? 'success' : 'error');
        }
    }
    
    updateStatistics(stats) {
        document.getElementById('total-messages').textContent = stats.total_messages;
        document.getElementById('json-messages').textContent = stats.json_messages;
        document.getElementById('text-messages').textContent = stats.text_messages;
    }
    
    async clearMessages() {
        if (!confirm('Are you sure you want to clear all messages?')) {
            return;
        }
        
        try {
            const response = await fetch('/api/clear-messages', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' }
            });
            
            const result = await response.json();
            
            if (result.success) {
                this.messages = [];
                document.getElementById('messages-container').innerHTML = `
                    <div class="empty-state">
                        <div class="empty-icon">
                            <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
                                <path d="M9 19c-5 0-8-3-8-8s3-8 8-8 8 3 8 8"/>
                                <path d="M17 12h.01"/>
                                <path d="M10 15l5-5"/>
                            </svg>
                        </div>
                        <h3>Messages cleared</h3>
                        <p>Stream is ready for new messages</p>
                    </div>
                `;
                this.updateStatistics({total_messages: 0, json_messages: 0, text_messages: 0});
                this.showToast('Messages cleared', 'success');
            }
        } catch (error) {
            this.showToast('Failed to clear messages: ' + error.message, 'error');
        }
    }
    
    exportMessages() {
        if (this.messages.length === 0) {
            this.showToast('No messages to export', 'warning');
            return;
        }
        
        const dataStr = JSON.stringify(this.messages, null, 2);
        const dataBlob = new Blob([dataStr], {type: 'application/json'});
        const url = URL.createObjectURL(dataBlob);
        
        const link = document.createElement('a');
        link.href = url;
        link.download = `kafka-messages-${new Date().toISOString().split('T')[0]}.json`;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        
        URL.revokeObjectURL(url);
        this.showToast(`Exported ${this.messages.length} messages`, 'success');
    }
    
    toggleTheme() {
        document.body.classList.toggle('light-theme');
        const isLight = document.body.classList.contains('light-theme');
        localStorage.setItem('theme', isLight ? 'light' : 'dark');
    }
    
    updateUI() {
        // Load saved theme
        const savedTheme = localStorage.getItem('theme');
        if (savedTheme === 'light') {
            document.body.classList.add('light-theme');
        }
        
        // Initial filter
        this.filterMessages();
    }
    
    showToast(message, type = 'info') {
        const toast = document.getElementById('status-toast');
        const toastBody = toast.querySelector('.toast-body');
        
        toastBody.textContent = message;
        
        // Update toast styling based on type
        toast.className = 'toast';
        if (type === 'success') {
            toast.classList.add('bg-success');
        } else if (type === 'error') {
            toast.classList.add('bg-danger');
        } else if (type === 'warning') {
            toast.classList.add('bg-warning');
        } else {
            toast.classList.add('bg-info');
        }
        
        const bsToast = new bootstrap.Toast(toast);
        bsToast.show();
    }
    
    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
}

// Global functions for HTML onclick handlers
let app;

function testConnection() {
    app.testConnection();
}

function refreshTopics() {
    app.refreshTopics();
}

function sendMessage() {
    app.sendMessage();
}

function startConsumer() {
    app.startConsumer();
}

function stopConsumer() {
    app.stopConsumer();
}

function clearMessages() {
    app.clearMessages();
}

function exportMessages() {
    app.exportMessages();
}

function toggleTheme() {
    app.toggleTheme();
}

// Initialize app when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    app = new KafkaTestTool();
});