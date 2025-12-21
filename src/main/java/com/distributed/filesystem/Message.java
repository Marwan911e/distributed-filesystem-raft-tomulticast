package com.distributed.filesystem;

import java.io.Serializable;

/**
 * Represents a message in the distributed file system
 */
public class Message implements Serializable, Comparable<Message> {
    private static final long serialVersionUID = 1L;
    
    private String messageId;
    private String senderId;
    private FileOperation operation;
    private String fileName;
    private byte[] fileData; // For upload operations
    private int timestamp; // Logical clock timestamp
    private MessageType type;
    
    public enum MessageType {
        REQUEST,
        ACK
    }
    
    public Message(String messageId, String senderId, FileOperation operation, 
                   String fileName, int timestamp, MessageType type) {
        this.messageId = messageId;
        this.senderId = senderId;
        this.operation = operation;
        this.fileName = fileName;
        this.timestamp = timestamp;
        this.type = type;
    }
    
    public Message(String messageId, String senderId, FileOperation operation, 
                   String fileName, byte[] fileData, int timestamp, MessageType type) {
        this(messageId, senderId, operation, fileName, timestamp, type);
        this.fileData = fileData;
    }
    
    // Getters
    public String getMessageId() { return messageId; }
    public String getSenderId() { return senderId; }
    public FileOperation getOperation() { return operation; }
    public String getFileName() { return fileName; }
    public byte[] getFileData() { return fileData; }
    public int getTimestamp() { return timestamp; }
    public MessageType getType() { return type; }
    
    @Override
    public int compareTo(Message other) {
        // Compare by timestamp first, then by messageId as tiebreaker
        int timestampCompare = Integer.compare(this.timestamp, other.timestamp);
        if (timestampCompare != 0) {
            return timestampCompare;
        }
        return this.messageId.compareTo(other.messageId);
    }
    
    @Override
    public String toString() {
        return String.format("Message[%s, %s, %s, %s, ts=%d, type=%s]", 
            messageId, senderId, operation, fileName, timestamp, type);
    }
}

