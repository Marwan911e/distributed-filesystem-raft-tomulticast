package com.distributed.filesystem;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * RMI interface for node communication
 */
public interface NodeInterface extends Remote {
    /**
     * Receive a multicast message from another node
     */
    void receiveMessage(Message message) throws RemoteException;
    
    /**
     * Get node information
     */
    NodeInfo getNodeInfo() throws RemoteException;
    
    /**
     * Check if node is alive (for leader election)
     */
    boolean isAlive() throws RemoteException;
}

