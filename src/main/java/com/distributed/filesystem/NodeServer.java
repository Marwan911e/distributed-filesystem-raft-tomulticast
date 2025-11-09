package com.distributed.filesystem;

import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Server class to start a node
 */
public class NodeServer {
    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Usage: NodeServer <nodeId> <hostname> <port> <storageDir> [node1:host1:port1] [node2:host2:port2] ...");
            System.exit(1);
        }
        
        String nodeId = args[0];
        String hostname = args[1];
        int port = Integer.parseInt(args[2]);
        String storageDir = args[3];
        
        // Parse other nodes
        List<NodeInfo> allNodes = new ArrayList<>();
        allNodes.add(new NodeInfo(nodeId, hostname, port));
        
        for (int i = 4; i < args.length; i++) {
            String[] parts = args[i].split(":");
            if (parts.length == 3) {
                allNodes.add(new NodeInfo(parts[0], parts[1], Integer.parseInt(parts[2])));
            }
        }
        
        try {
            // Create registry if it doesn't exist
            try {
                Registry registry = LocateRegistry.getRegistry(port);
                registry.list(); // Test if registry exists
            } catch (Exception e) {
                // Registry doesn't exist, create it
                LocateRegistry.createRegistry(port);
            }
            
            // Create node
            Node node = new Node(new NodeInfo(nodeId, hostname, port), allNodes, storageDir);
            
            // Bind node
            String url = "rmi://" + hostname + ":" + port + "/Node";
            Naming.rebind(url, node);
            
            System.out.println("Node " + nodeId + " is ready at " + hostname + ":" + port);
            System.out.println("Storage directory: " + storageDir);
            System.out.println("Total nodes: " + allNodes.size());
            
            // Interactive command interface
            Scanner scanner = new Scanner(System.in);
            System.out.println("\nCommands:");
            System.out.println("  upload <filename> <filepath>  - Upload a file");
            System.out.println("  download <filename>           - Download a file");
            System.out.println("  delete <filename>              - Delete a file");
            System.out.println("  search <filename>              - Search for a file");
            System.out.println("  list                           - List stored files");
            System.out.println("  clock                          - Show logical clock");
            System.out.println("  leader                         - Show current leader");
            System.out.println("  quit                           - Exit");
            System.out.println();
            
            while (true) {
                System.out.print("[" + nodeId + "] > ");
                String command = scanner.nextLine().trim();
                
                if (command.isEmpty()) continue;
                
                String[] parts = command.split("\\s+");
                String cmd = parts[0].toLowerCase();
                
                try {
                    switch (cmd) {
                        case "upload":
                            if (parts.length >= 3) {
                                String fileName = parts[1];
                                String filePath = parts[2];
                                java.io.File file = new java.io.File(filePath);
                                if (file.exists()) {
                                    java.io.FileInputStream fis = new java.io.FileInputStream(file);
                                    byte[] data = fis.readAllBytes();
                                    fis.close();
                                    node.uploadFile(fileName, data);
                                } else {
                                    System.out.println("File not found: " + filePath);
                                }
                            } else {
                                System.out.println("Usage: upload <filename> <filepath>");
                            }
                            break;
                            
                        case "download":
                            if (parts.length >= 2) {
                                node.downloadFile(parts[1]);
                            } else {
                                System.out.println("Usage: download <filename>");
                            }
                            break;
                            
                        case "delete":
                            if (parts.length >= 2) {
                                node.deleteFile(parts[1]);
                            } else {
                                System.out.println("Usage: delete <filename>");
                            }
                            break;
                            
                        case "search":
                            if (parts.length >= 2) {
                                node.searchFile(parts[1]);
                            } else {
                                System.out.println("Usage: search <filename>");
                            }
                            break;
                            
                        case "list":
                            List<String> files = node.getStoredFiles();
                            System.out.println("Stored files (" + files.size() + "):");
                            for (String f : files) {
                                System.out.println("  - " + f);
                            }
                            break;
                            
                        case "clock":
                            System.out.println("Logical clock: " + node.getLogicalClock());
                            break;
                            
                        case "leader":
                            System.out.println("Current leader: " + node.getLeaderId());
                            break;
                            
                        case "quit":
                        case "exit":
                            System.out.println("Shutting down...");
                            scanner.close();
                            System.exit(0);
                            break;
                            
                        default:
                            System.out.println("Unknown command: " + cmd);
                    }
                } catch (Exception e) {
                    System.err.println("Error: " + e.getMessage());
                    e.printStackTrace();
                }
            }
            
        } catch (Exception e) {
            System.err.println("Node server error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}

