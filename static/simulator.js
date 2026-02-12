/**
 * Distributed Systems Simulator - Frontend JavaScript
 * 
 * Handles WebSocket communication, canvas rendering, and user interactions.
 */

// Connect to WebSocket
const socket = io();

// State
let clusterState = null;
let canvas, ctx;
let nodePositions = [];
let animationFrame = null;

// Configuration
const CONFIG = {
    nodeRadius: 50,
    nodeSpacing: 200,
    connectionColor: 'rgba(48, 54, 61, 0.6)',
    leaderColor: '#58a6ff',
    followerColor: '#3fb950',
    candidateColor: '#d29922',
    failedColor: '#f85149',
    textColor: '#e6edf3',
    secondaryText: '#8b949e',
    regionColors: {
        'us-east': '#58a6ff',
        'us-west': '#3fb950',
        'eu-central': '#a371f7',
        'asia-pacific': '#d29922'
    }
};

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    initCanvas();
    initSocket();
    initControls();
});

function initCanvas() {
    canvas = document.getElementById('canvas');
    ctx = canvas.getContext('2d');
    resizeCanvas();
    window.addEventListener('resize', resizeCanvas);
}

function resizeCanvas() {
    const container = canvas.parentElement;
    canvas.width = container.clientWidth;
    canvas.height = container.clientHeight;
    calculateNodePositions();
}

function calculateNodePositions() {
    if (!clusterState || !clusterState.nodes) return;
    
    const nodes = Object.keys(clusterState.nodes);
    const centerX = canvas.width / 2;
    const centerY = canvas.height / 2;
    const radius = Math.min(canvas.width, canvas.height) * 0.35;
    
    nodePositions = nodes.map((nodeId, i) => {
        const angle = (i * 2 * Math.PI / nodes.length) - Math.PI / 2;
        return {
            nodeId,
            x: centerX + radius * Math.cos(angle),
            y: centerY + radius * Math.sin(angle)
        };
    });
}

function initSocket() {
    socket.on('connect', () => {
        console.log('Connected to server');
    });
    
    socket.on('disconnect', () => {
        console.log('Disconnected from server');
    });
    
    socket.on('cluster_state', (state) => {
        clusterState = state;
        calculateNodePositions();
        updateUI();
        render();
    });
}

function initControls() {
    // Command input
    document.getElementById('commandInput').addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            executeCommand();
        }
    });
    
    document.getElementById('executeBtn').addEventListener('click', executeCommand);
    
    // Add node
    document.getElementById('addNodeBtn').addEventListener('click', addNode);
    
    // Reset
    document.getElementById('resetBtn').addEventListener('click', resetSimulation);
    
    // Tick
    document.getElementById('tickBtn').addEventListener('click', tickSimulation);
    
    // Partition controls
    document.getElementById('partitionBtn').addEventListener('click', createPartition);
    document.getElementById('healBtn').addEventListener('click', healPartitions);
    
    // Node interactions
    canvas.addEventListener('mousemove', handleMouseMove);
    canvas.addEventListener('click', handleCanvasClick);
}

async function executeCommand() {
    const input = document.getElementById('commandInput');
    const command = input.value.trim();
    
    if (!command) return;
    
    try {
        const response = await fetch('/api/command', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ command })
        });
        
        const result = await response.json();
        console.log('Command result:', result);
        
        input.value = '';
    } catch (error) {
        console.error('Command error:', error);
    }
}

async function addNode() {
    const input = document.getElementById('newNodeId');
    const nodeId = input.value.trim() || `node-${Date.now()}`;
    const region = document.getElementById('regionSelect').value;
    
    try {
        const response = await fetch('/api/nodes', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ node_id: nodeId, region: region })
        });
        
        const result = await response.json();
        console.log('Node created:', result);
        
        input.value = '';
    } catch (error) {
        console.error('Add node error:', error);
    }
}

async function createPartition() {
    const region1 = document.getElementById('partitionRegion1').value;
    const region2 = document.getElementById('partitionRegion2').value;
    
    if (region1 === region2) {
        alert('Please select different regions');
        return;
    }
    
    try {
        const response = await fetch('/api/partitions', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ region1, region2 })
        });
        
        const result = await response.json();
        console.log('Partition created:', result);
    } catch (error) {
        console.error('Create partition error:', error);
    }
}

async function healPartitions() {
    try {
        const response = await fetch('/api/partitions', {
            method: 'DELETE'
        });
        
        const result = await response.json();
        console.log('Partitions healed:', result);
    } catch (error) {
        console.error('Heal partitions error:', error);
    }
}

async function deleteNode(nodeId) {
    try {
        const response = await fetch(`/api/nodes/${nodeId}`, {
            method: 'DELETE'
        });
        
        const result = await response.json();
        console.log('Node deleted:', result);
    } catch (error) {
        console.error('Delete node error:', error);
    }
}

async function injectFault(nodeId, type) {
    try {
        const response = await fetch('/api/fault/inject', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ node_id: nodeId, type })
        });
        
        const result = await response.json();
        console.log('Fault injected:', result);
    } catch (error) {
        console.error('Inject fault error:', error);
    }
}

async function resetSimulation() {
    try {
        const response = await fetch('/api/reset', {
            method: 'POST'
        });
        
        const result = await response.json();
        console.log('Simulation reset:', result);
    } catch (error) {
        console.error('Reset error:', error);
    }
}

async function tickSimulation() {
    try {
        const response = await fetch('/api/simulation/tick', {
            method: 'POST'
        });
    } catch (error) {
        console.error('Tick error:', error);
    }
}

function updateUI() {
    if (!clusterState) return;
    
    // Update stats
    document.getElementById('statNodes').textContent = Object.keys(clusterState.nodes).length;
    document.getElementById('statLeader').textContent = clusterState.leader || '-';
    document.getElementById('statEvents').textContent = clusterState.event_log.length;
    
    // Find max term
    let maxTerm = 0;
    Object.values(clusterState.nodes).forEach(node => {
        if (node.term > maxTerm) maxTerm = node.term;
    });
    document.getElementById('statTerm').textContent = maxTerm;
    
    // Update partition stat
    const partitionCount = clusterState.partitions ? clusterState.partitions.length : 0;
    const partitionStat = document.getElementById('statPartitions');
    if (partitionStat) {
        partitionStat.textContent = partitionCount;
    }
    
    // Update node list
    updateNodeList();
    
    // Update events log
    updateEventsLog();
    
    // Update partition list
    updatePartitionList();
}

function updateNodeList() {
    const list = document.getElementById('nodeList');
    list.innerHTML = '';
    
    if (!clusterState || !clusterState.nodes) return;
    
    Object.entries(clusterState.nodes).forEach(([nodeId, node]) => {
        const item = document.createElement('div');
        item.className = 'node-item';
        
        const stateClass = node.state;
        
        item.innerHTML = `
            <div class="node-info">
                <div class="node-state-indicator ${stateClass}"></div>
                <div>
                    <div class="node-name">${nodeId}</div>
                    <div class="node-term">Term ${node.term} · ${node.commit_index}/${node.log_length} committed</div>
                </div>
            </div>
            <div class="node-actions">
                ${node.is_failed 
                    ? `<button class="node-btn" onclick="injectFault('${nodeId}', 'recover')">Recover</button>`
                    : `<button class="node-btn danger" onclick="injectFault('${nodeId}', 'fail')">Fail</button>`
                }
                <button class="node-btn danger" onclick="deleteNode('${nodeId}')">✕</button>
            </div>
        `;
        
        list.appendChild(item);
    });
}

function updateEventsLog() {
    const log = document.getElementById('eventsLog');
    log.innerHTML = '';
    
    if (!clusterState || !clusterState.event_log) return;
    
    const events = clusterState.event_log.slice(-20).reverse();
    
    events.forEach(event => {
        const item = document.createElement('div');
        
        let eventClass = 'message';
        if (event.type.includes('election')) eventClass = 'election';
        else if (event.type.includes('command')) eventClass = 'command';
        else if (event.type.includes('fault') || event.type.includes('fail')) eventClass = 'fault';
        else if (event.type.includes('partition')) eventClass = 'fault';
        
        item.className = `event-item ${eventClass}`;
        
        const time = new Date(event.timestamp * 1000).toLocaleTimeString();
        
        let text = event.type;
        if (event.data.node) text += `: ${event.data.node}`;
        if (event.data.term) text += ` (term ${event.data.term})`;
        if (event.data.region1) text += `: ${event.data.region1} ↔ ${event.data.region2}`;
        
        item.innerHTML = `
            <div class="event-time">${time}</div>
            <div class="event-text">${text}</div>
        `;
        
        log.appendChild(item);
    });
}

function updatePartitionList() {
    const list = document.getElementById('partitionList');
    list.innerHTML = '';
    
    if (!clusterState || !clusterState.partitions) return;
    
    clusterState.partitions.forEach(partition => {
        const item = document.createElement('div');
        item.className = 'partition-item';
        item.innerHTML = `
            <span>${partition.region1} ↔ ${partition.region2}</span>
        `;
        list.appendChild(item);
    });
}

// Canvas rendering
function render() {
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    
    if (!clusterState || !clusterState.nodes) {
        requestAnimationFrame(render);
        return;
    }
    
    // Draw connections
    drawConnections();
    
    // Draw nodes
    drawNodes();
    
    requestAnimationFrame(render);
}

function drawConnections() {
    const leader = clusterState.leader;
    const partitions = clusterState.partitions || [];
    
    nodePositions.forEach((pos1, i) => {
        nodePositions.slice(i + 1).forEach(pos2 => {
            const node1 = clusterState.nodes[pos1.nodeId];
            const node2 = clusterState.nodes[pos2.nodeId];
            
            // Check if there's a partition between these nodes' regions
            let isPartitioned = false;
            if (node1 && node2 && partitions.length > 0) {
                isPartitioned = partitions.some(p => 
                    (p.region1 === node1.region && p.region2 === node2.region) ||
                    (p.region1 === node2.region && p.region2 === node1.region)
                );
            }
            
            ctx.beginPath();
            ctx.moveTo(pos1.x, pos1.y);
            ctx.lineTo(pos2.x, pos2.y);
            
            if (isPartitioned) {
                ctx.strokeStyle = 'rgba(248, 81, 73, 0.5)';
                ctx.lineWidth = 3;
                ctx.setLineDash([10, 5]);
            } else if (node1?.is_leader && node2?.state === 'follower') {
                ctx.strokeStyle = 'rgba(88, 166, 255, 0.4)';
                ctx.lineWidth = 2;
                ctx.setLineDash([]);
            } else {
                ctx.strokeStyle = CONFIG.connectionColor;
                ctx.lineWidth = 1;
                ctx.setLineDash([]);
            }
            
            ctx.stroke();
            ctx.setLineDash([]);
        });
    });
}

function drawNodes() {
    nodePositions.forEach(pos => {
        const node = clusterState.nodes[pos.nodeId];
        if (!node) return;
        
        const { x, y } = pos;
        const radius = CONFIG.nodeRadius;
        
        // Glow effect for leader
        if (node.state === 'leader') {
            const gradient = ctx.createRadialGradient(x, y, radius, x, y, radius * 2);
            gradient.addColorStop(0, 'rgba(88, 166, 255, 0.3)');
            gradient.addColorStop(1, 'rgba(88, 166, 255, 0)');
            ctx.fillStyle = gradient;
            ctx.beginPath();
            ctx.arc(x, y, radius * 2, 0, Math.PI * 2);
            ctx.fill();
        }
        
        // Node circle
        ctx.beginPath();
        ctx.arc(x, y, radius, 0, Math.PI * 2);
        
        let fillColor;
        if (node.is_failed) {
            fillColor = CONFIG.failedColor;
        } else {
            switch (node.state) {
                case 'leader': fillColor = CONFIG.leaderColor; break;
                case 'candidate': fillColor = CONFIG.candidateColor; break;
                default: fillColor = CONFIG.followerColor;
            }
        }
        
        ctx.fillStyle = fillColor;
        ctx.fill();
        
        // Region-colored border
        const regionColor = CONFIG.regionColors[node.region] || CONFIG.leaderColor;
        ctx.strokeStyle = regionColor;
        ctx.lineWidth = 4;
        ctx.stroke();
        
        // Inner white border
        ctx.strokeStyle = 'rgba(255, 255, 255, 0.2)';
        ctx.lineWidth = 2;
        ctx.stroke();
        
        // Node ID
        ctx.fillStyle = CONFIG.textColor;
        ctx.font = 'bold 14px Outfit, sans-serif';
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillText(pos.nodeId, x, y - 8);
        
        // State
        ctx.fillStyle = CONFIG.secondaryText;
        ctx.font = '10px JetBrains Mono, monospace';
        ctx.fillText(node.state.toUpperCase(), x, y + 10);
        
        // Region label
        ctx.fillText(node.region, x, y + 24);
    });
}

// Mouse handling for tooltips
function handleMouseMove(e) {
    const rect = canvas.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const y = e.clientY - rect.top;
    
    const tooltip = document.getElementById('tooltip');
    let hoveredNode = null;
    
    nodePositions.forEach(pos => {
        const node = clusterState?.nodes[pos.nodeId];
        if (!node) return;
        
        const dx = x - pos.x;
        const dy = y - pos.y;
        const dist = Math.sqrt(dx * dx + dy * dy);
        
        if (dist <= CONFIG.nodeRadius) {
            hoveredNode = { ...pos, ...node };
        }
    });
    
    if (hoveredNode) {
        tooltip.classList.add('visible');
        tooltip.style.left = (hoveredNode.x + 60) + 'px';
        tooltip.style.top = (hoveredNode.y - 40) + 'px';
        
        document.getElementById('tooltipNodeId').textContent = hoveredNode.nodeId;
        
        const stateEl = document.getElementById('tooltipState');
        stateEl.textContent = hoveredNode.state.toUpperCase();
        stateEl.className = `tooltip-state ${hoveredNode.state}`;
        
        if (hoveredNode.is_failed) {
            stateEl.textContent = 'FAILED';
            stateEl.className = 'tooltip-state failed';
        }
        
        document.getElementById('tooltipTerm').textContent = hoveredNode.term;
        document.getElementById('tooltipCommit').textContent = hoveredNode.commit_index;
        document.getElementById('tooltipLog').textContent = hoveredNode.log_length;
        document.getElementById('tooltipDelay').textContent = 
            hoveredNode.network_delay > 0 ? `${(hoveredNode.network_delay * 1000).toFixed(0)}ms` : '0ms';
    } else {
        tooltip.classList.remove('visible');
    }
}

function handleCanvasClick(e) {
    const rect = canvas.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const y = e.clientY - rect.top;
    
    nodePositions.forEach(pos => {
        const node = clusterState?.nodes[pos.nodeId];
        if (!node) return;
        
        const dx = x - pos.x;
        const dy = y - pos.y;
        const dist = Math.sqrt(dx * dx + dy * dy);
        
        if (dist <= CONFIG.nodeRadius) {
            console.log('Clicked node:', pos.nodeId, node);
        }
    });
}

// Make functions available globally for onclick handlers
window.injectFault = injectFault;
window.deleteNode = deleteNode;
