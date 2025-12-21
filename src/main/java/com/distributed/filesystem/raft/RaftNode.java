package com.distributed.filesystem.raft;

import com.distributed.filesystem.Node;
import com.distributed.filesystem.NodeInfo;

import java.rmi.RemoteException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Raft-based leader election implementation (Bonus part)
 * This extends the Node class to add Raft leader election
 */
public class RaftNode extends Node {
    private static final long serialVersionUID = 1L;
    
    // Raft state
    public enum RaftState {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }
    
    private RaftState state;
    private AtomicInteger currentTerm;
    private String votedFor; // nodeId that received vote in current term
    private AtomicInteger lastLogIndex;
    
    // Election timeout (milliseconds)
    private static final int ELECTION_TIMEOUT_MIN = 150;
    private static final int ELECTION_TIMEOUT_MAX = 300;
    
    public RaftNode(NodeInfo nodeInfo, List<NodeInfo> allNodes, String storageDirectory) 
            throws RemoteException {
        super(nodeInfo, allNodes, storageDirectory);
        this.state = RaftState.FOLLOWER;
        this.currentTerm = new AtomicInteger(0);
        this.votedFor = null;
        this.lastLogIndex = new AtomicInteger(0);
        
        // Start election timer
        startElectionTimer();
    }
    
    /**
     * Start election timer - if timeout occurs, become candidate and start election
     */
    private void startElectionTimer() {
        Thread timerThread = new Thread(() -> {
            while (true) {
                try {
                    int timeout = ELECTION_TIMEOUT_MIN + 
                        (int)(Math.random() * (ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN));
                    Thread.sleep(timeout);
                    
                    // If still follower after timeout, start election
                    if (state == RaftState.FOLLOWER) {
                        startElection();
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        timerThread.setDaemon(true);
        timerThread.start();
    }
    
    /**
     * Start leader election
     */
    private synchronized void startElection() {
        try {
            System.out.println("[" + getNodeInfo().getNodeId() + "] Starting election (term " + 
                              currentTerm.incrementAndGet() + ")");
            
            state = RaftState.CANDIDATE;
            votedFor = getNodeInfo().getNodeId();
            
            // Request votes from all other nodes
            int votes = 1; // Vote for self
            List<NodeInfo> allNodes = getAllNodes();
            
            for (NodeInfo node : allNodes) {
                if (!node.getNodeId().equals(getNodeInfo().getNodeId())) {
                    // In a real implementation, we would send RequestVote RPC
                    // For simplicity, we'll use a simple majority vote
                    // Assume nodes vote yes if they haven't voted in this term
                    votes++;
                }
            }
            
            // Check if we have majority
            if (votes > allNodes.size() / 2) {
                becomeLeader();
            } else {
                // Didn't get majority, remain candidate and retry
                state = RaftState.CANDIDATE;
            }
        } catch (RemoteException e) {
            System.err.println("Error in election: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Become the leader
     */
    private void becomeLeader() {
        try {
            state = RaftState.LEADER;
            setLeaderId(getNodeInfo().getNodeId());
            System.out.println("[" + getNodeInfo().getNodeId() + "] Elected as LEADER (term " + 
                              currentTerm.get() + ")");
            
            // Start sending heartbeats
            startHeartbeat();
        } catch (RemoteException e) {
            System.err.println("Error becoming leader: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Start sending periodic heartbeats to maintain leadership
     */
    private void startHeartbeat() {
        Thread heartbeatThread = new Thread(() -> {
            while (state == RaftState.LEADER) {
                try {
                    // Send heartbeat to all followers
                    // In a real implementation, this would be AppendEntries RPC
                    Thread.sleep(50); // Heartbeat interval
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        heartbeatThread.setDaemon(true);
        heartbeatThread.start();
    }
    
    /**
     * Handle vote request (simplified)
     */
    public synchronized boolean requestVote(int term, String candidateId) {
        if (term > currentTerm.get()) {
            currentTerm.set(term);
            state = RaftState.FOLLOWER;
            votedFor = null;
        }
        
        if (term == currentTerm.get() && (votedFor == null || votedFor.equals(candidateId))) {
            votedFor = candidateId;
            return true;
        }
        
        return false;
    }
    
    /**
     * Handle heartbeat/AppendEntries (simplified)
     */
    public synchronized void receiveHeartbeat(int term, String leaderId) {
        if (term >= currentTerm.get()) {
            currentTerm.set(term);
            state = RaftState.FOLLOWER;
            setLeaderId(leaderId);
            // Reset election timer
        }
    }
    
    public RaftState getState() {
        return state;
    }
    
    public int getCurrentTerm() {
        return currentTerm.get();
    }
}

