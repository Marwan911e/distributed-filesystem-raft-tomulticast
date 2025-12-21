package com.distributed.filesystem;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.rmi.Naming;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Test scenario demonstrating TO-Multicasting with concurrent upload and delete
 */
public class TestScenario {
    
    public static void main(String[] args) {
        System.out.println("=== Distributed File System Test Scenario ===");
        System.out.println("Testing Totally Ordered Multicast with concurrent operations\n");
        
        // Node configuration
        List<NodeInfo> nodes = new ArrayList<>();
        nodes.add(new NodeInfo("Node1", "localhost", 1099));
        nodes.add(new NodeInfo("Node2", "localhost", 1100));
        nodes.add(new NodeInfo("Node3", "localhost", 1101));
        nodes.add(new NodeInfo("Node4", "localhost", 1102));
        
        try {
            // Wait for nodes to be ready
            System.out.println("Waiting for nodes to be ready...");
            Thread.sleep(2000);
            
            // Get node stubs
            NodeInterface node1 = (NodeInterface) Naming.lookup("rmi://localhost:1099/Node");
            NodeInterface node2 = (NodeInterface) Naming.lookup("rmi://localhost:1100/Node");
            NodeInterface node3 = (NodeInterface) Naming.lookup("rmi://localhost:1101/Node");
            NodeInterface node4 = (NodeInterface) Naming.lookup("rmi://localhost:1102/Node");
            
            // Create test file
            String testFileName = "test.txt";
            String testContent = "This is a test file for distributed file system";
            File testFile = new File("test.txt");
            FileWriter writer = new FileWriter(testFile);
            writer.write(testContent);
            writer.close();
            byte[] testData = java.nio.file.Files.readAllBytes(testFile.toPath());
            
            System.out.println("\n=== Scenario: Concurrent Upload and Delete ===");
            System.out.println("Node A will upload file, Node B will delete the same file concurrently");
            System.out.println("This tests Totally Ordered Multicast to ensure consistency\n");
            
            // Start upload from Node1
            Thread uploadThread = new Thread(() -> {
                try {
                    System.out.println("[Node1] Starting upload of " + testFileName);
                    node1.receiveMessage(new Message(
                        "Node1-1", "Node1", FileOperation.UPLOAD, testFileName, 
                        testData, 1, Message.MessageType.REQUEST
                    ));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            
            // Start delete from Node2 (concurrent)
            Thread deleteThread = new Thread(() -> {
                try {
                    System.out.println("[Node2] Starting delete of " + testFileName);
                    node2.receiveMessage(new Message(
                        "Node2-1", "Node2", FileOperation.DELETE, testFileName,
                        null, 1, Message.MessageType.REQUEST
                    ));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            
            // Start both operations concurrently
            uploadThread.start();
            deleteThread.start();
            
            uploadThread.join();
            deleteThread.join();
            
            // Wait for operations to complete
            Thread.sleep(3000);
            
            System.out.println("\n=== Test Complete ===");
            System.out.println("Check the node outputs to verify:");
            System.out.println("1. All nodes received both requests");
            System.out.println("2. Requests were ordered consistently across all nodes");
            System.out.println("3. All nodes executed operations in the same order");
            
        } catch (Exception e) {
            System.err.println("Test scenario error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

