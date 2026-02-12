"""
Distributed Systems Simulator - Main Server

This module provides the Flask application with WebSocket support
for the distributed systems simulator.

Features:
- Raft Consensus Algorithm implementation
- Multi-region cluster support
- Network partition simulation (CAP theorem)
- Fault injection (node failures, network delays, message loss)
- Real-time visualization
- REST API for cluster control
"""

import json
import logging
import threading
import time
from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit

from raft import RaftCluster, RaftNode, NodeState

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Flask and SocketIO
app = Flask(__name__)
app.config['SECRET_KEY'] = 'distributed-systems-secret'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Global cluster instance
cluster = RaftCluster()

# Simulation control
simulation_running = False
simulation_thread = None

# Region configuration
REGIONS = {
    'us-east': {'latency': 0, 'color': '#58a6ff'},
    'us-west': {'latency': 50, 'color': '#3fb950'},
    'eu-central': {'latency': 80, 'color': '#a371f7'},
    'asia-pacific': {'latency': 150, 'color': '#d29922'}
}


def run_simulation():
    """Run the simulation loop."""
    global simulation_running
    
    while simulation_running:
        try:
            # Tick the cluster
            cluster.tick()
            
            # Broadcast state to all clients
            state = cluster.get_cluster_state()
            socketio.emit('cluster_state', state)
            
            # Sleep for simulation tick
            socketio.sleep(0.1)
        except Exception as e:
            logger.error(f"Simulation error: {e}")


@socketio.on('connect')
def handle_connect():
    """Handle client connection."""
    logger.info(f"Client connected: {request.sid}")
    # Send initial state
    emit('cluster_state', cluster.get_cluster_state())


@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection."""
    logger.info(f"Client disconnected: {request.sid}")


@app.route('/')
def index():
    """Main page."""
    return render_template('index.html')


@app.route('/api/nodes', methods=['GET'])
def get_nodes():
    """Get all nodes in the cluster."""
    state = cluster.get_cluster_state()
    nodes = state['nodes']
    return jsonify([
        {"node_id": nid, **info} for nid, info in nodes.items()
    ])


@app.route('/api/nodes', methods=['POST'])
def create_node():
    """Create a new node."""
    data = request.get_json() or {}
    node_id = data.get('node_id', f"node-{len(cluster.nodes)}")
    region = data.get('region', 'us-east')
    
    if node_id in cluster.nodes:
        return jsonify({"error": "Node already exists"}), 400
    
    node = cluster.add_node(node_id)
    node.region = region
    node.region_latency = REGIONS.get(region, {}).get('latency', 0)
    
    return jsonify({"node_id": node_id, "region": region, "status": "created"})


@app.route('/api/nodes/<node_id>', methods=['DELETE'])
def delete_node(node_id):
    """Delete a node."""
    if node_id not in cluster.nodes:
        return jsonify({"error": "Node not found"}), 404
    
    cluster.remove_node(node_id)
    return jsonify({"node_id": node_id, "status": "deleted"})


@app.route('/api/fault/inject', methods=['POST'])
def inject_fault():
    """Inject a fault into a node."""
    data = request.get_json()
    node_id = data.get('node_id')
    fault_type = data.get('type')
    value = data.get('value')
    
    if node_id not in cluster.nodes:
        return jsonify({"error": "Node not found"}), 404
    
    node = cluster.nodes[node_id]
    node.inject_fault(fault_type, value)
    
    return jsonify({
        "node_id": node_id,
        "fault_type": fault_type,
        "status": "injected"
    })


@app.route('/api/command', methods=['POST'])
def submit_command():
    """Submit a command to the cluster."""
    data = request.get_json()
    command = data.get('command')
    
    if not command:
        return jsonify({"error": "No command provided"}), 400
    
    success, message = cluster.submit_command(command)
    
    return jsonify({
        "command": command,
        "success": success,
        "message": message
    })


@app.route('/api/state', methods=['GET'])
def get_state():
    """Get full cluster state."""
    return jsonify(cluster.get_cluster_state())


@app.route('/api/reset', methods=['POST'])
def reset_simulation():
    """Reset the simulation."""
    global simulation_running
    
    simulation_running = False
    if simulation_thread:
        simulation_thread.join()
    
    cluster.reset()
    
    # Create initial cluster with 5 nodes in different regions
    regions = ['us-east', 'us-west', 'eu-central', 'asia-pacific', 'us-east']
    for i in range(5):
        node = cluster.add_node(f"node-{i}")
        node.region = regions[i]
        node.region_latency = REGIONS[regions[i]]['latency']
    
    simulation_running = True
    simulation_thread = socketio.start_background_task(run_simulation)
    
    return jsonify({"status": "reset"})


@app.route('/api/regions', methods=['GET'])
def get_regions():
    """Get available regions."""
    return jsonify(REGIONS)


@app.route('/api/partitions', methods=['GET'])
def get_partitions():
    """Get current network partitions."""
    return jsonify(cluster.get_partitions())


@app.route('/api/partitions', methods=['POST'])
def create_partition():
    """Create a network partition."""
    data = request.get_json()
    region1 = data.get('region1')
    region2 = data.get('region2')
    
    success = cluster.create_partition(region1, region2)
    
    return jsonify({"status": "created" if success else "failed"})


@app.route('/api/partitions', methods=['DELETE'])
def heal_partition():
    """Heal all network partitions."""
    cluster.heal_partitions()
    return jsonify({"status": "healed"})


@app.route('/api/simulation/start', methods=['POST'])
def start_simulation():
    """Start the simulation."""
    global simulation_running, simulation_thread
    
    if simulation_running:
        return jsonify({"status": "already running"})
    
    simulation_running = True
    simulation_thread = socketio.start_background_task(run_simulation)
    
    return jsonify({"status": "started"})


@app.route('/api/simulation/stop', methods=['POST'])
def stop_simulation():
    """Stop the simulation."""
    global simulation_running
    
    simulation_running = False
    
    return jsonify({"status": "stopped"})


@app.route('/api/simulation/tick', methods=['POST'])
def tick_simulation():
    """Advance simulation by one tick."""
    cluster.tick()
    return jsonify({"status": "ticked"})


def main():
    """Main entry point."""
    # Initialize cluster with 5 nodes in different regions
    regions = ['us-east', 'us-west', 'eu-central', 'asia-pacific', 'us-east']
    for i in range(5):
        node = cluster.add_node(f"node-{i}")
        node.region = regions[i]
        node.region_latency = REGIONS[regions[i]]['latency']
    
    # Start simulation
    global simulation_running
    simulation_running = True
    
    # Start background task
    simulation_thread = socketio.start_background_task(run_simulation)
    
    logger.info("Starting Distributed Systems Simulator...")
    logger.info("Open http://localhost:5000 in your browser")
    
    socketio.run(app, host='0.0.0.0', port=5000, debug=True, allow_unsafe_werkzeug=True)


if __name__ == '__main__':
    main()
