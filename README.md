# Distributed Systems Simulator

An interactive real-time playground for exploring distributed systems algorithms, consensus protocols, and fault tolerance patterns.

## Overview

This project implements a visual, interactive simulator for distributed systems concepts including:

- **Raft Consensus Algorithm** - Leader election, log replication, commitment
- **Multi-Region Deployment** - Nodes deployed across US East, US West, EU Central, and Asia-Pacific with configurable latency
- **Network Partition Handling** - Split-brain scenarios, CAP theorem tradeoffs
- **Fault Injection** - Node failures, network delays, message loss
- **Real-time Visualization** - See consensus happen in real-time via WebSocket

## Architecture

- **Simulation Engine**: Python-based Raft implementation (~800 lines)
- **Real-time Updates**: WebSocket-based state propagation
- **Frontend**: Interactive canvas visualization with node tooltips
- **REST API**: Control simulation, inject faults, query state

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run the simulator
python3 server.py

# Open http://localhost:5000 in your browser
```

## Features

### Leader Election
Watch as nodes compete to become leader using the Raft consensus algorithm. The election process includes:
- Randomized election timeouts to prevent split votes
- Term-based voting
- Majority quorum for election victory

### Multi-Region Simulation
Nodes are distributed across geographic regions:
- **US East** (0ms latency) - Blue
- **US West** (50ms latency) - Green  
- **EU Central** (80ms latency) - Purple
- **Asia Pacific** (150ms latency) - Orange

### Network Partitions
Simulate network partitions between regions to explore CAP theorem tradeoffs:
- Create partitions to isolate regions
- Observe split-brain scenarios
- Heal partitions and watch recovery

### Fault Injection
- **Fail/Recover Nodes** - Simulate node crashes and recoveries
- **Network Delay** - Add artificial latency to specific nodes
- **Message Loss** - Simulate unreliable networks

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/nodes` | GET | List all nodes |
| `/api/nodes` | POST | Create a new node |
| `/api/nodes/<id>` | DELETE | Remove a node |
| `/api/fault/inject` | POST | Inject a fault |
| `/api/command` | POST | Submit a command |
| `/api/state` | GET | Get cluster state |
| `/api/partitions` | GET/POST/DELETE | Manage partitions |
| `/api/reset` | POST | Reset simulation |

## Technical Implementation

### Raft Algorithm Features
- Leader election with randomized timeouts
- Log replication with consistency checks
- Term-based voting
- Commit index tracking
- Volatile state management

### Network Simulation
- Per-region latency simulation
- Message loss probability
- Partition blocking between regions
- Asynchronous message delivery

### Real-time Visualization
- Canvas-based node rendering
- Color-coded node states (leader/follower/candidate/failed)
- Region-colored borders
- Partition visualization with dashed lines
- Interactive tooltips
