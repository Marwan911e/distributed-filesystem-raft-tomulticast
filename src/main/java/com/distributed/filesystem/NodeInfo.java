package com.distributed.filesystem;

import java.io.Serializable;

/**
 * Represents information about a node in the distributed system
 */
public class NodeInfo implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private String nodeId;
    private String hostname;
    private int port;
    
    public NodeInfo(String nodeId, String hostname, int port) {
        this.nodeId = nodeId;
        this.hostname = hostname;
        this.port = port;
    }
    
    public String getNodeId() { return nodeId; }
    public String getHostname() { return hostname; }
    public int getPort() { return port; }
    
    @Override
    public String toString() {
        return String.format("NodeInfo[%s, %s:%d]", nodeId, hostname, port);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        NodeInfo nodeInfo = (NodeInfo) obj;
        return nodeId.equals(nodeInfo.nodeId);
    }
    
    @Override
    public int hashCode() {
        return nodeId.hashCode();
    }
}

