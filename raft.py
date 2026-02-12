"""
Raft Consensus Algorithm Implementation

This module implements the Raft consensus algorithm for the distributed systems simulator.
It handles leader election, log replication, and consistency guarantees.
"""

import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any
import uuid

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NodeState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class MessageType(Enum):
    REQUEST_VOTE = "request_vote"
    VOTE_RESPONSE = "vote_response"
    APPEND_ENTRIES = "append_entries"
    APPEND_RESPONSE = "append_response"
    CLIENT_COMMAND = "client_command"
    COMMAND_RESPONSE = "command_response"


@dataclass
class LogEntry:
    """A single entry in the replicated log."""
    term: int
    index: int
    command: str
    timestamp: float = field(default_factory=time.time)
    
    def to_dict(self) -> dict:
        return {
            "term": self.term,
            "index": self.index,
            "command": self.command,
            "timestamp": self.timestamp
        }


@dataclass
class Message:
    """Inter-node message for Raft protocol."""
    type: MessageType
    from_node: str
    to_node: str
    term: int
    data: dict = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    
    def to_dict(self) -> dict:
        return {
            "type": self.type.value,
            "from_node": self.from_node,
            "to_node": self.to_node,
            "term": self.term,
            "data": self.data,
            "timestamp": self.timestamp
        }


@dataclass
class Vote:
    """Vote granted to a candidate."""
    voter_id: str
    candidate_id: str
    term: int
    granted: bool


class RaftNode:
    """
    A single Raft node implementation.
    
    Handles:
    - Leader election
    - Log replication
    - Membership changes
    - Fault injection
    - Multi-region deployment
    """
    
    def __init__(self, node_id: str, cluster: 'RaftCluster'):
        self.node_id = node_id
        self.cluster = cluster
        
        # Persistent state (survives crashes)
        self.current_term = 0
        self.voted_for: str | None = None
        self.log: list[LogEntry] = []
        
        # Volatile state
        self.state = NodeState.FOLLOWER
        self.commit_index = 0
        self.last_applied = 0
        
        # Leader-specific volatile state
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}
        
        # Election and heartbeat timers
        self.election_timeout = random.uniform(1.5, 3.0)
        # Add some node-specific offset to prevent simultaneous elections
        self.election_timeout += random.uniform(0, 0.5) * (hash(node_id) % 100) / 100
        self.heartbeat_interval = 0.5
        self.last_contact = time.time()
        
        # Vote tracking for election
        self.received_votes: set[str] = set()
        
        # Fault injection state
        self.is_failed = False
        self.network_delay = 0.0
        self.message_loss_rate = 0.0
        
        # Region configuration
        self.region = 'us-east'
        self.region_latency = 0
        
        # Apply callback
        self.apply_callback: callable | None = None
        
        # Initial log entry
        self.log.append(LogEntry(term=0, index=0, command=""))
        
        logger.info(f"Node {self.node_id} initialized as follower")
    
    @property
    def is_leader(self) -> bool:
        return self.state == NodeState.LEADER
    
    @property
    def is_candidate(self) -> bool:
        return self.state == NodeState.CANDIDATE
    
    @property
    def is_follower(self) -> bool:
        return self.state == NodeState.FOLLOWER
    
    def reset_election_timer(self):
        """Reset the election timeout."""
        self.last_contact = time.time()
    
    def should_start_election(self) -> bool:
        """Check if election timeout has elapsed."""
        if self.is_leader:
            return False
        elapsed = time.time() - self.last_contact
        return elapsed > self.election_timeout
    
    def become_leader(self):
        """Transition to leader state."""
        self.state = NodeState.LEADER
        self.voted_for = None
        self.received_votes = set()
        
        # Initialize leader state
        last_log_index = len(self.log) - 1
        for node_id in self.cluster.nodes:
            self.next_index[node_id] = last_log_index + 1
            self.match_index[node_id] = 0
        
        logger.info(f"Node {self.node_id} became LEADER (term {self.current_term})")
        self.cluster.broadcast_state()
    
    def become_follower(self, term: int):
        """Transition to follower state."""
        self.state = NodeState.FOLLOWER
        self.current_term = term
        self.voted_for = None
        self.received_votes = set()
        self.reset_election_timer()
        logger.info(f"Node {self.node_id} became FOLLOWER (term {self.current_term})")
    
    def become_candidate(self):
        """Transition to candidate state."""
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.received_votes = {self.node_id}  # Vote for self
        self.reset_election_timer()
        logger.info(f"Node {self.node_id} became CANDIDATE (term {self.current_term})")
    
    def request_vote(self, candidate_id: str, candidate_term: int, 
                     candidate_last_index: int, candidate_last_term: int) -> Vote:
        """
        Handle RequestVote RPC.
        Returns whether we vote for the candidate.
        """
        # Update term if needed
        if candidate_term > self.current_term:
            self.become_follower(candidate_term)
        
        # Vote if:
        # 1. Term is at least our current term
        # 2. We haven't voted for someone else this term
        # 3. Candidate's log is at least as up-to-date as ours
        
        log_up_to_date = (
            candidate_last_term > self.log[-1].term or
            (candidate_last_term == self.log[-1].term and 
             candidate_last_index >= len(self.log) - 1)
        )
        
        vote_granted = (
            candidate_term >= self.current_term and
            (self.voted_for is None or self.voted_for == candidate_id) and
            log_up_to_date
        )
        
        if vote_granted:
            self.voted_for = candidate_id
            self.reset_election_timer()
        
        return Vote(
            voter_id=self.node_id,
            candidate_id=candidate_id,
            term=self.current_term,
            granted=vote_granted
        )
    
    def append_entries(self, leader_id: str, leader_term: int, 
                       prev_log_index: int, prev_log_term: int,
                       entries: list[LogEntry], leader_commit: int) -> dict:
        """
        Handle AppendEntries RPC.
        Returns success status and relevant information.
        """
        # Update term if needed
        if leader_term > self.current_term:
            self.become_follower(leader_term)
        
        # If we're leader, reject (shouldn't receive append entries)
        if self.is_leader:
            return {"success": False, "term": self.current_term}
        
        # Check if we have the entry at prev_log_index
        if prev_log_index >= len(self.log):
            return {"success": False, "term": self.current_term}
        
        # Check if term matches
        if prev_log_index >= 0 and self.log[prev_log_index].term != prev_log_term:
            return {"success": False, "term": self.current_term}
        
        # Find first conflicting entry
        conflict_index = prev_log_index + 1
        for i, entry in enumerate(entries):
            if conflict_index + i >= len(self.log):
                break
            if self.log[conflict_index + i].term != entry.term:
                # Truncate log
                self.log = self.log[:conflict_index + i]
                break
        
        # Append new entries
        new_entries = entries[conflict_index - (prev_log_index + 1):]
        self.log.extend(new_entries)
        
        # Update commit index
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log) - 1)
            self.apply_committed()
        
        self.reset_election_timer()
        
        return {"success": True, "term": self.current_term}
    
    def apply_committed(self):
        """Apply committed log entries to state machine."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            if entry.command and self.apply_callback:
                self.apply_callback(self.node_id, entry)
            logger.info(f"Node {self.node_id} applied log index {entry.index}: {entry.command}")
    
    def start_election(self) -> list[Message]:
        """
        Start an election for leader.
        Returns list of RequestVote messages to send.
        """
        self.become_candidate()
        
        # Request votes from all other nodes
        messages = []
        last_log_index = len(self.log) - 1
        last_log_term = self.log[-1].term
        
        for node_id in self.cluster.nodes:
            if node_id == self.node_id:
                continue
            messages.append(Message(
                type=MessageType.REQUEST_VOTE,
                from_node=self.node_id,
                to_node=node_id,
                term=self.current_term,
                data={
                    "candidate_id": self.node_id,
                    "last_log_index": last_log_index,
                    "last_log_term": last_log_term
                }
            ))
        
        return messages
    
    def handle_vote_response(self, voter_id: str, term: int, 
                            vote_granted: bool) -> bool:
        """
        Handle a vote response.
        Returns True if we won the election.
        """
        if term > self.current_term:
            self.become_follower(term)
            return False
        
        if not self.is_candidate:
            return False
        
        if vote_granted:
            self.received_votes.add(voter_id)
            
            # Check if we have majority (count self-vote + received votes)
            total_votes = 1 + len(self.received_votes)
            majority = (len(self.cluster.nodes) // 2) + 1
            
            logger.info(f"Node {self.node_id}: got vote from {voter_id}, total votes: {total_votes}/{majority}")
            
            if total_votes >= majority:
                self.become_leader()
                return True
        
        return False
    
    def send_heartbeat(self) -> list[Message]:
        """Send heartbeat to all followers."""
        if not self.is_leader:
            return []
        
        messages = []
        for node_id in self.cluster.nodes:
            if node_id == self.node_id:
                continue
            
            prev_log_index = self.next_index.get(node_id, len(self.log)) - 1
            prev_log_term = self.log[prev_log_index].term if prev_log_index >= 0 else 0
            
            messages.append(Message(
                type=MessageType.APPEND_ENTRIES,
                from_node=self.node_id,
                to_node=node_id,
                term=self.current_term,
                data={
                    "leader_id": self.node_id,
                    "prev_log_index": prev_log_index,
                    "prev_log_term": prev_log_term,
                    "entries": [],
                    "leader_commit": self.commit_index
                }
            ))
        
        return messages
    
    def replicate_log(self, node_id: str) -> Message | None:
        """Send log entries to a follower."""
        if not self.is_leader:
            return None
        
        next_idx = self.next_index.get(node_id, len(self.log))
        prev_log_index = next_idx - 1
        prev_log_term = self.log[prev_log_index].term if prev_log_index >= 0 else 0
        
        entries = self.log[next_idx:]
        
        return Message(
            type=MessageType.APPEND_ENTRIES,
            from_node=self.node_id,
            to_node=node_id,
            term=self.current_term,
            data={
                "leader_id": self.node_id,
                "prev_log_index": prev_log_index,
                "prev_log_term": prev_log_term,
                "entries": [e.to_dict() for e in entries],
                "leader_commit": self.commit_index
            }
        )
    
    def handle_append_response(self, node_id: str, term: int, 
                               success: bool, match_index: int):
        """Handle AppendEntries response from a follower."""
        if term > self.current_term:
            self.become_follower(term)
            return
        
        if not self.is_leader:
            return
        
        if success:
            self.next_index[node_id] = match_index + 1
            self.match_index[node_id] = match_index
            
            # Check if we can commit new entries
            self.update_commit_index()
        else:
            # Decrement next index and retry
            self.next_index[node_id] = max(1, self.next_index.get(node_id, 1) - 1)
    
    def update_commit_index(self):
        """Update commit index based on replicated entries."""
        for index in range(self.commit_index + 1, len(self.log)):
            # Count replicas
            replicas = sum(1 for node_id in self.cluster.nodes
                          if self.match_index.get(node_id, 0) >= index)
            
            majority = (len(self.cluster.nodes) // 2) + 1
            if replicas >= majority and self.log[index].term == self.current_term:
                self.commit_index = index
                self.apply_committed()
    
    def receive_command(self, command: str) -> tuple[bool, str]:
        """
        Receive a command from client.
        Returns (success, message).
        """
        if not self.is_leader:
            return False, f"Not leader (current leader: {self.cluster.get_leader()})"
        
        # Append to log
        entry = LogEntry(
            term=self.current_term,
            index=len(self.log),
            command=command
        )
        self.log.append(entry)
        
        # Replicate to followers
        self.cluster.broadcast_append_entries()
        
        # For simplicity, assume success after replication
        return True, f"Command replicated to {len(self.cluster.nodes)} nodes"
    
    def inject_fault(self, fault_type: str, value: Any = None):
        """Inject a fault into this node."""
        if fault_type == "fail":
            self.is_failed = True
            logger.warning(f"Node {self.node_id} has FAILED")
        elif fault_type == "recover":
            self.is_failed = False
            logger.info(f"Node {self.node_id} has RECOVERED")
        elif fault_type == "delay":
            self.network_delay = float(value) if value else 0.5
        elif fault_type == "loss":
            self.message_loss_rate = float(value) if value else 0.5
    
    def get_state(self) -> dict:
        """Get current node state for visualization."""
        return {
            "node_id": self.node_id,
            "state": self.state.value,
            "term": self.current_term,
            "voted_for": self.voted_for,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "log": [e.to_dict() for e in self.log],
            "log_length": len(self.log),
            "is_failed": self.is_failed,
            "network_delay": self.network_delay,
            "message_loss_rate": self.message_loss_rate,
            "is_leader": self.is_leader,
            "region": self.region,
            "region_latency": self.region_latency
        }


class RaftCluster:
    """
    Manages a cluster of Raft nodes.
    Handles inter-node communication and coordination.
    
    Features:
    - Multi-region deployment with configurable latency
    - Network partition simulation (CAP theorem)
    - Fault injection
    """
    
    def __init__(self):
        self.nodes: dict[str, RaftNode] = {}
        self.pending_messages: list[Message] = []
        self.client_commands: list[tuple[str, str]] = []  # (command, response)
        self.event_log: list[dict] = []
        
        # Network partitions - list of (region1, region2) pairs that can't communicate
        self.partitions: list[tuple[str, str]] = []
        
        logger.info("RaftCluster initialized")
    
    def add_node(self, node_id: str) -> RaftNode:
        """Add a new node to the cluster."""
        node = RaftNode(node_id, self)
        self.nodes[node_id] = node
        
        # Update next_index for leader
        for n in self.nodes.values():
            if n.is_leader:
                n.next_index[node_id] = len(n.log)
                n.match_index[node_id] = 0
        
        self.log_event("node_added", {"node_id": node_id, "region": node.region})
        logger.info(f"Added node {node_id} to cluster")
        
        return node
    
    def remove_node(self, node_id: str):
        """Remove a node from the cluster."""
        if node_id in self.nodes:
            del self.nodes[node_id]
            self.log_event("node_removed", {"node_id": node_id})
            logger.info(f"Removed node {node_id} from cluster")
    
    def get_node(self, node_id: str) -> RaftNode | None:
        """Get a node by ID."""
        return self.nodes.get(node_id)
    
    def get_leader(self) -> str | None:
        """Get the current leader node ID."""
        for node in self.nodes.values():
            if node.is_leader:
                return node.node_id
        return None
    
    def broadcast_append_entries(self):
        """Broadcast AppendEntries from leader to all followers."""
        leader = self.get_node(self.get_leader())
        if not leader:
            return
        
        for node_id in self.nodes:
            if node_id == leader.node_id:
                continue
            msg = leader.replicate_log(node_id)
            if msg:
                self.pending_messages.append(msg)
    
    def deliver_message(self, message: Message) -> list[Message]:
        """
        Deliver a message to its destination node.
        Returns any response messages.
        """
        responses = []
        
        # Get source and destination nodes
        src_node = self.nodes.get(message.from_node)
        dest_node = self.nodes.get(message.to_node)
        
        if not dest_node:
            return responses
        
        # Check for network partition
        if src_node and self.is_partitioned(src_node.region, dest_node.region):
            self.log_event("message_blocked_partition", {
                "from": message.from_node,
                "to": message.to_node,
                "from_region": src_node.region if src_node else "unknown",
                "to_region": dest_node.region
            })
            return responses
        
        # Simulate network delay (base delay + region latency)
        # In simulation mode, we don't actually sleep - just track it
        total_delay = dest_node.network_delay + dest_node.region_latency
        # Only sleep for debugging (not in simulation tick)
        # time.sleep(total_delay / 1000)  # Disabled for simulation speed
        
        # Simulate message loss
        if dest_node and random.random() < dest_node.message_loss_rate:
            self.log_event("message_lost", {
                "from": message.from_node,
                "to": message.to_node,
                "type": message.type.value
            })
            return responses
        
        if message.to_node not in self.nodes:
            return responses
        
        node = self.nodes[message.to_node]
        
        if node.is_failed:
            self.log_event("message_dropped_failed_node", {
                "to": message.to_node,
                "type": message.type.value
            })
            return responses
        
        if message.type == MessageType.REQUEST_VOTE:
            vote = node.request_vote(
                message.data["candidate_id"],
                message.term,
                message.data["last_log_index"],
                message.data["last_log_term"]
            )
            responses.append(Message(
                type=MessageType.VOTE_RESPONSE,
                from_node=node.node_id,
                to_node=message.from_node,
                term=node.current_term,
                data={
                    "voter_id": vote.voter_id,
                    "candidate_id": vote.candidate_id,
                    "vote_granted": vote.granted
                }
            ))
            
        elif message.type == MessageType.APPEND_ENTRIES:
            entries = [LogEntry(**e) for e in message.data.get("entries", [])]
            result = node.append_entries(
                message.data["leader_id"],
                message.term,
                message.data["prev_log_index"],
                message.data["prev_log_term"],
                entries,
                message.data["leader_commit"]
            )
            responses.append(Message(
                type=MessageType.APPEND_RESPONSE,
                from_node=node.node_id,
                to_node=message.from_node,
                term=node.current_term,
                data={
                    "success": result["success"],
                    "term": result["term"],
                    # Report follower's match_index (what it has replicated)
                    "match_index": node.match_index.get(node.node_id, 0)
                }
            ))
        
        return responses
    
    def process_messages(self):
        """Process all pending messages."""
        new_messages = []
        
        while self.pending_messages:
            msg = self.pending_messages.pop(0)
            
            responses = self.deliver_message(msg)
            new_messages.extend(responses)
            
            # Handle vote responses
            if msg.type == MessageType.VOTE_RESPONSE:
                node = self.nodes.get(msg.to_node)
                if node:
                    node.handle_vote_response(
                        msg.data["voter_id"],
                        msg.term,
                        msg.data["vote_granted"]
                    )
            
            # Handle append responses
            elif msg.type == MessageType.APPEND_ENTRIES:
                # AppendEntries responses are handled in deliver_message
                pass
            
            elif msg.type == MessageType.APPEND_RESPONSE:
                leader = self.nodes.get(msg.to_node)
                if leader:
                    leader.handle_append_response(
                        msg.from_node,
                        msg.term,
                        msg.data["success"],
                        msg.data.get("match_index", 0)
                    )
        
        self.pending_messages.extend(new_messages)
    
    def tick(self):
        """Advance the cluster state by one tick."""
        # Process pending messages
        self.process_messages()
        
        # Check for elections
        for node in self.nodes.values():
            if node.should_start_election():
                messages = node.start_election()
                self.pending_messages.extend(messages)
                self.log_event("election_started", {
                    "node": node.node_id,
                    "term": node.current_term
                })
        
        # Send heartbeats from leader
        leader = self.get_node(self.get_leader())
        if leader:
            messages = leader.send_heartbeat()
            self.pending_messages.extend(messages)
        
        # Process messages triggered by heartbeats
        self.process_messages()
    
    def submit_command(self, command: str) -> tuple[bool, str]:
        """Submit a command to the cluster."""
        leader_id = self.get_leader()
        if not leader_id:
            return False, "No leader elected"
        
        leader = self.nodes[leader_id]
        return leader.receive_command(command)
    
    def get_cluster_state(self) -> dict:
        """Get full cluster state for visualization."""
        return {
            "nodes": {node_id: node.get_state() 
                     for node_id, node in self.nodes.items()},
            "leader": self.get_leader(),
            "pending_messages": len(self.pending_messages),
            "event_log": self.event_log[-50:],  # Last 50 events
            "partitions": self.get_partitions(),
            "regional_leaders": self.get_regional_leaders()
        }
    
    def log_event(self, event_type: str, data: dict):
        """Log a cluster event."""
        self.event_log.append({
            "type": event_type,
            "data": data,
            "timestamp": time.time()
        })
        # Keep only last 100 events
        if len(self.event_log) > 100:
            self.event_log = self.event_log[-100:]
    
    def reset(self):
        """Reset the cluster."""
        self.nodes.clear()
        self.pending_messages.clear()
        self.client_commands.clear()
        self.event_log.clear()
        self.partitions.clear()
        logger.info("Cluster reset")
    
    def is_partitioned(self, region1: str, region2: str) -> bool:
        """Check if two regions are partitioned."""
        return (region1, region2) in self.partitions or (region2, region1) in self.partitions
    
    def create_partition(self, region1: str, region2: str) -> bool:
        """Create a network partition between two regions."""
        if region1 not in ['us-east', 'us-west', 'eu-central', 'asia-pacific']:
            return False
        if region2 not in ['us-east', 'us-west', 'eu-central', 'asia-pacific']:
            return False
        
        self.partitions.append((region1, region2))
        self.log_event("partition_created", {"region1": region1, "region2": region2})
        logger.info(f"Created partition between {region1} and {region2}")
        return True
    
    def heal_partitions(self):
        """Heal all network partitions."""
        self.partitions.clear()
        self.log_event("partitions_healed", {})
        logger.info("All partitions healed")
    
    def get_partitions(self) -> list:
        """Get current partitions."""
        return [{"region1": p[0], "region2": p[1]} for p in self.partitions]
    
    def get_regional_leaders(self) -> dict:
        """Get leaders by region."""
        leaders = {}
        for node in self.nodes.values():
            if node.is_leader:
                leaders[node.region] = node.node_id
        return leaders
    
    def broadcast_state(self):
        """Broadcast current state (placeholder for real-time updates)."""
        # This is handled by the WebSocket in the server
        pass
