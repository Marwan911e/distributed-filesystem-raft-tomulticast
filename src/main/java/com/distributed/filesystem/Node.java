package com.distributed.filesystem;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a node in the distributed file system
 */
public class Node extends UnicastRemoteObject implements NodeInterface {
    private static final long serialVersionUID = 1L;
    
    private NodeInfo nodeInfo;
    private List<NodeInfo> allNodes;
    private Map<String, NodeInterface> nodeStubs;
    
    // Logical clock
    private AtomicInteger logicalClock;
    
    // TO-Multicasting: Request queue and ACK tracking
    private PriorityBlockingQueue<Message> requestQueue;
    private Map<String, Set<String>> acksReceived; // messageId -> set of nodeIds that ACKed
    private Set<String> executedMessages; // Track executed messages to avoid duplicates
    
    // File storage
    private String storageDirectory;
    private Map<String, FileMetadata> fileMetadata; // fileName -> metadata
    
    // Leader election
    private String leaderId;
    private Random random;
    
    // Failure detection
    private Set<String> failedNodes; // Track failed nodes
    
    // Request ID generator
    private AtomicInteger requestIdCounter;
    
    public Node(NodeInfo nodeInfo, List<NodeInfo> allNodes, String storageDirectory) throws RemoteException {
        this.nodeInfo = nodeInfo;
        this.allNodes = new ArrayList<>(allNodes);
        this.nodeStubs = new ConcurrentHashMap<>();
        this.logicalClock = new AtomicInteger(0);
        this.requestQueue = new PriorityBlockingQueue<>();
        this.acksReceived = new ConcurrentHashMap<>();
        this.executedMessages = ConcurrentHashMap.newKeySet();
        this.storageDirectory = storageDirectory;
        this.fileMetadata = new ConcurrentHashMap<>();
        this.random = new Random();
        this.requestIdCounter = new AtomicInteger(0);
        this.failedNodes = ConcurrentHashMap.newKeySet();
        
        // Create storage directory
        File dir = new File(storageDirectory);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        
        // Initialize leader deterministically (all nodes pick the same leader)
        if (!allNodes.isEmpty()) {
            // Sort nodes by ID to ensure consistent ordering across all nodes
            List<NodeInfo> sortedNodes = new ArrayList<>(allNodes);
            sortedNodes.sort(Comparator.comparing(NodeInfo::getNodeId));
            leaderId = sortedNodes.get(0).getNodeId(); // Pick first node alphabetically
        }
        
        System.out.println("Node " + nodeInfo.getNodeId() + " initialized. Leader: " + leaderId);
    }
    
    /**
     * Update logical clock and return new value
     */
    private int updateLogicalClock(int receivedTimestamp) {
        return logicalClock.updateAndGet(current -> Math.max(current, receivedTimestamp) + 1);
    }
    
    /**
     * Get current logical clock value
     */
    public int getLogicalClock() {
        return logicalClock.get();
    }
    
    /**
     * Get all nodes list (for Raft extension)
     */
    public List<NodeInfo> getAllNodes() {
        return new ArrayList<>(allNodes);
    }
    
    /**
     * Multicast a message to all nodes (including self)
     */
    private void multicastMessage(Message message) {
        System.out.println("[" + nodeInfo.getNodeId() + "] Multicasting: " + message);
        
        // Send to all active nodes including self (exclude failed nodes)
        for (NodeInfo node : allNodes) {
            // Skip failed nodes
            if (failedNodes.contains(node.getNodeId())) {
                continue;
            }
            
            if (node.getNodeId().equals(nodeInfo.getNodeId())) {
                // Send to self (synchronously to maintain order)
                try {
                    receiveMessage(message);
                } catch (RemoteException e) {
                    // Should not happen for self
                    e.printStackTrace();
                }
            } else {
                // Send to other nodes
                try {
                    NodeInterface stub = getNodeStub(node);
                    if (stub != null) {
                        stub.receiveMessage(message);
                    } else {
                        // Stub lookup failed, mark node as failed
                        markNodeAsFailed(node.getNodeId());
                    }
                } catch (RemoteException e) {
                    System.err.println("[" + nodeInfo.getNodeId() + "] Failed to send message to " + 
                                     node.getNodeId() + ": " + e.getMessage());
                    // Mark node as failed
                    markNodeAsFailed(node.getNodeId());
                }
            }
        }
    }
    
    /**
     * Get or create RMI stub for a node
     */
    private NodeInterface getNodeStub(NodeInfo node) {
        // Don't try to get stub for failed nodes
        if (failedNodes.contains(node.getNodeId())) {
            return null;
        }
        
        NodeInterface stub = nodeStubs.get(node.getNodeId());
        if (stub != null) {
            return stub;
        }
        
        // Try to create new stub
        try {
            String url = "rmi://" + node.getHostname() + ":" + node.getPort() + "/Node";
            stub = (NodeInterface) java.rmi.Naming.lookup(url);
            nodeStubs.put(node.getNodeId(), stub);
            return stub;
        } catch (Exception e) {
            System.err.println("[" + nodeInfo.getNodeId() + "] Failed to get stub for " + 
                             node.getNodeId() + ": " + e.getMessage());
            // Mark node as failed (outside of computeIfAbsent to avoid recursive update)
            markNodeAsFailed(node.getNodeId());
            return null;
        }
    }
    
    /**
     * Mark a node as failed and handle leader re-election if needed
     */
    private synchronized void markNodeAsFailed(String nodeId) {
        if (failedNodes.add(nodeId)) {
            System.out.println("[" + nodeInfo.getNodeId() + "] Node " + nodeId + " marked as failed");
            
            // Remove stub from cache (safely, outside of any computeIfAbsent)
            // Use a separate operation to avoid recursive update
            NodeInterface removed = nodeStubs.remove(nodeId);
            
            // If the failed node is the leader, elect a new leader
            if (nodeId.equals(leaderId)) {
                electNewLeader();
            }
        }
    }
    
    /**
     * Elect a new leader from active nodes
     */
    private synchronized void electNewLeader() {
        // Get list of active nodes (excluding failed ones)
        List<NodeInfo> activeNodes = new ArrayList<>();
        for (NodeInfo node : allNodes) {
            if (!failedNodes.contains(node.getNodeId())) {
                activeNodes.add(node);
            }
        }
        
        if (!activeNodes.isEmpty()) {
            // Elect new leader deterministically (all nodes pick the same leader)
            List<NodeInfo> sortedActiveNodes = new ArrayList<>(activeNodes);
            sortedActiveNodes.sort(Comparator.comparing(NodeInfo::getNodeId));
            NodeInfo newLeader = sortedActiveNodes.get(0); // Pick first node alphabetically
            leaderId = newLeader.getNodeId();
            System.out.println("[" + nodeInfo.getNodeId() + "] New leader elected: " + leaderId);
        } else {
            System.err.println("[" + nodeInfo.getNodeId() + "] No active nodes available for leader election!");
            leaderId = null;
        }
    }
    
    /**
     * Receive a message from another node (RMI call)
     */
    @Override
    public synchronized void receiveMessage(Message message) throws RemoteException {
        int receivedTimestamp = message.getTimestamp();
        updateLogicalClock(receivedTimestamp);
        
        if (message.getType() == Message.MessageType.REQUEST) {
            // Add request to queue
            requestQueue.offer(message);
            System.out.println("[" + nodeInfo.getNodeId() + "] Received REQUEST: " + message + 
                            " (Clock: " + logicalClock.get() + ")");
            
            // Multicast ACK
            Message ack = new Message(message.getMessageId(), nodeInfo.getNodeId(), 
                                     message.getOperation(), message.getFileName(), 
                                     updateLogicalClock(0), Message.MessageType.ACK);
            multicastMessage(ack);
            
        } else if (message.getType() == Message.MessageType.ACK) {
            // Record ACK
            acksReceived.computeIfAbsent(message.getMessageId(), k -> ConcurrentHashMap.newKeySet())
                        .add(message.getSenderId());
            System.out.println("[" + nodeInfo.getNodeId() + "] Received ACK from " + 
                            message.getSenderId() + " for " + message.getMessageId());
        }
        
        // Process queue
        processQueue();
    }
    
    /**
     * Process the request queue and execute ready requests
     */
    private void processQueue() {
        // Calculate number of active nodes (excluding failed ones)
        int activeNodeCount = (int) allNodes.stream()
            .filter(node -> !failedNodes.contains(node.getNodeId()))
            .count();
        
        while (!requestQueue.isEmpty()) {
            Message request = requestQueue.peek();
            if (request == null) break;
            
            // Check if all active nodes have ACKed
            Set<String> acks = acksReceived.get(request.getMessageId());
            if (acks != null && acks.size() == activeNodeCount) {
                // Remove from queue
                requestQueue.poll();
                
                // Execute if not already executed
                if (!executedMessages.contains(request.getMessageId())) {
                    executeRequest(request);
                    executedMessages.add(request.getMessageId());
                }
            } else {
                // Can't process this request yet, wait for more ACKs
                break;
            }
        }
    }
    
    /**
     * Execute a request
     */
    private void executeRequest(Message request) {
        System.out.println("[" + nodeInfo.getNodeId() + "] Executing: " + request);
        
        switch (request.getOperation()) {
            case UPLOAD:
                executeUpload(request);
                break;
            case DELETE:
                executeDelete(request);
                break;
            case DOWNLOAD:
                if (nodeInfo.getNodeId().equals(leaderId)) {
                    executeDownload(request);
                } else {
                    System.out.println("[" + nodeInfo.getNodeId() + "] Not leader, skipping DOWNLOAD");
                }
                break;
            case SEARCH:
                if (nodeInfo.getNodeId().equals(leaderId)) {
                    executeSearch(request);
                } else {
                    System.out.println("[" + nodeInfo.getNodeId() + "] Not leader, skipping SEARCH");
                }
                break;
        }
    }
    
    /**
     * Execute upload operation
     */
    private void executeUpload(Message request) {
        try {
            File file = new File(storageDirectory, request.getFileName());
            FileOutputStream fos = new FileOutputStream(file);
            fos.write(request.getFileData());
            fos.close();
            
            fileMetadata.put(request.getFileName(), 
                new FileMetadata(request.getFileName(), request.getFileData().length, 
                               request.getSenderId()));
            
            System.out.println("[" + nodeInfo.getNodeId() + "] Uploaded file: " + 
                            request.getFileName() + " (" + request.getFileData().length + " bytes)");
        } catch (IOException e) {
            System.err.println("[" + nodeInfo.getNodeId() + "] Upload failed: " + e.getMessage());
        }
    }
    
    /**
     * Execute delete operation
     */
    private void executeDelete(Message request) {
        File file = new File(storageDirectory, request.getFileName());
        if (file.exists() && file.delete()) {
            fileMetadata.remove(request.getFileName());
            System.out.println("[" + nodeInfo.getNodeId() + "] Deleted file: " + request.getFileName());
        } else {
            System.out.println("[" + nodeInfo.getNodeId() + "] File not found for deletion: " + 
                            request.getFileName());
        }
    }
    
    /**
     * Execute download operation (leader only)
     */
    private void executeDownload(Message request) {
        File file = new File(storageDirectory, request.getFileName());
        if (file.exists()) {
            try {
                FileInputStream fis = new FileInputStream(file);
                byte[] data = fis.readAllBytes();
                fis.close();
                
                System.out.println("[" + nodeInfo.getNodeId() + "] DOWNLOAD: File " + 
                                request.getFileName() + " found (" + data.length + " bytes)");
                // In a real implementation, we would send the file data back to the requester
                // For simplicity, we just log it here
            } catch (IOException e) {
                System.err.println("[" + nodeInfo.getNodeId() + "] Download failed: " + e.getMessage());
            }
        } else {
            System.out.println("[" + nodeInfo.getNodeId() + "] DOWNLOAD: File " + 
                            request.getFileName() + " not found");
        }
    }
    
    /**
     * Execute search operation (leader only)
     */
    private void executeSearch(Message request) {
        boolean found = fileMetadata.containsKey(request.getFileName());
        System.out.println("[" + nodeInfo.getNodeId() + "] SEARCH: File " + 
                        request.getFileName() + (found ? " found" : " not found"));
    }
    
    /**
     * Upload a file
     */
    public void uploadFile(String fileName, byte[] fileData) {
        String messageId = nodeInfo.getNodeId() + "-" + requestIdCounter.incrementAndGet();
        int timestamp = updateLogicalClock(0);
        
        Message request = new Message(messageId, nodeInfo.getNodeId(), 
                                     FileOperation.UPLOAD, fileName, fileData, timestamp, 
                                     Message.MessageType.REQUEST);
        
        multicastMessage(request);
    }
    
    /**
     * Delete a file
     */
    public void deleteFile(String fileName) {
        String messageId = nodeInfo.getNodeId() + "-" + requestIdCounter.incrementAndGet();
        int timestamp = updateLogicalClock(0);
        
        Message request = new Message(messageId, nodeInfo.getNodeId(), 
                                     FileOperation.DELETE, fileName, timestamp, 
                                     Message.MessageType.REQUEST);
        
        multicastMessage(request);
    }
    
    /**
     * Download a file (leader only)
     */
    public void downloadFile(String fileName) {
        String messageId = nodeInfo.getNodeId() + "-" + requestIdCounter.incrementAndGet();
        int timestamp = updateLogicalClock(0);
        
        Message request = new Message(messageId, nodeInfo.getNodeId(), 
                                     FileOperation.DOWNLOAD, fileName, timestamp, 
                                     Message.MessageType.REQUEST);
        
        multicastMessage(request);
    }
    
    /**
     * Search for a file (leader only)
     */
    public void searchFile(String fileName) {
        String messageId = nodeInfo.getNodeId() + "-" + requestIdCounter.incrementAndGet();
        int timestamp = updateLogicalClock(0);
        
        Message request = new Message(messageId, nodeInfo.getNodeId(), 
                                     FileOperation.SEARCH, fileName, timestamp, 
                                     Message.MessageType.REQUEST);
        
        multicastMessage(request);
    }
    
    @Override
    public NodeInfo getNodeInfo() throws RemoteException {
        return nodeInfo;
    }
    
    @Override
    public boolean isAlive() throws RemoteException {
        return true;
    }
    
    public String getLeaderId() {
        // If current leader is failed, elect a new one
        if (leaderId != null && failedNodes.contains(leaderId)) {
            electNewLeader();
        }
        return leaderId;
    }
    
    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
        System.out.println("[" + nodeInfo.getNodeId() + "] New leader: " + leaderId);
    }
    
    public List<String> getStoredFiles() {
        return new ArrayList<>(fileMetadata.keySet());
    }
    
    /**
     * File metadata class
     */
    private static class FileMetadata {
        String fileName;
        long size;
        String uploadedBy;
        
        FileMetadata(String fileName, long size, String uploadedBy) {
            this.fileName = fileName;
            this.size = size;
            this.uploadedBy = uploadedBy;
        }
    }
}

