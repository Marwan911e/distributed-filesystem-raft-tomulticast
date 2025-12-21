# CS425 Coursework Project Report

## Distributed File System with Totally Ordered Multicast

### 1. Introduction

This project implements a distributed file system using Java RMI with Totally Ordered Multicast (TO-Multicasting) to ensure consistency across multiple nodes. The system supports four basic file operations: Upload, Download, Delete, and Search.

### 2. Architecture and Design

#### 2.1 System Architecture

The system consists of at least 4 nodes, each running independently and communicating via Java RMI. Each node:

- Maintains its own file storage
- Keeps information about all other nodes (hostname, port)
- Implements TO-Multicasting for consistency
- Supports leader election for Download and Search operations

#### 2.2 Key Components

**Node.java**: Main node implementation

- Manages logical clock
- Implements TO-Multicasting with request queue
- Handles file operations
- Tracks ACKs from all nodes

**Message.java**: Communication protocol

- REQUEST: File operation request with timestamp
- ACK: Acknowledgment message

**NodeInterface.java**: RMI interface for remote communication

**FileOperation.java**: Enum defining operations (UPLOAD, DOWNLOAD, DELETE, SEARCH)

### 3. Implementation Details

#### 3.1 Logical Clock

Each node maintains a logical clock (`AtomicInteger`) that is updated according to Lamport's algorithm:

```java
private int updateLogicalClock(int receivedTimestamp) {
    return logicalClock.updateAndGet(current ->
        Math.max(current, receivedTimestamp) + 1);
}
```

- When sending: `clock = clock + 1`
- When receiving: `clock = max(clock, receivedTimestamp) + 1`

#### 3.2 Totally Ordered Multicast

**Request Flow:**

1. Node updates logical clock and creates timestamped request
2. Request is multicast to all nodes (including sender)
3. Each recipient:
   - Updates logical clock
   - Adds request to priority queue (ordered by timestamp, then messageId)
   - Multicasts ACK to all nodes
4. Request executes when:
   - It's at front of queue
   - All nodes have ACKed

**Key Functions:**

```java
// Multicast message to all nodes
private void multicastMessage(Message message)

// Receive and process message
public synchronized void receiveMessage(Message message)

// Process queue and execute ready requests
private void processQueue()
```

**Request Queue**: Uses `PriorityBlockingQueue<Message>` ordered by:

1. Timestamp (logical clock value)
2. Message ID (tiebreaker)

**ACK Tracking**: `Map<String, Set<String>>` tracks which nodes have ACKed each request.

#### 3.3 File Operations

**Upload (`executeUpload`)**:

- Writes file data to local storage
- Updates file metadata
- Executed by ALL nodes

**Delete (`executeDelete`)**:

- Removes file from local storage
- Updates metadata
- Executed by ALL nodes

**Download (`executeDownload`)**:

- Reads file from local storage
- Returns file data
- Executed by LEADER only

**Search (`executeSearch`)**:

- Checks if file exists locally
- Returns search result
- Executed by LEADER only

#### 3.4 Leader Election

**Basic Implementation**: Random leader selection at initialization.

**Raft Implementation (Bonus)**:

- `RaftNode.java` extends `Node` with Raft consensus
- States: FOLLOWER, CANDIDATE, LEADER
- Election timeout: 150-300ms
- Majority vote required for leadership

### 4. Function Explanations

#### 4.1 Node Initialization

```java
public Node(NodeInfo nodeInfo, List<NodeInfo> allNodes, String storageDirectory)
```

- Initializes node with ID, hostname, port
- Sets up storage directory
- Initializes logical clock, queues, and data structures
- Randomly selects initial leader

#### 4.2 Message Handling

```java
public synchronized void receiveMessage(Message message)
```

- Updates logical clock based on received timestamp
- If REQUEST: adds to queue, multicasts ACK
- If ACK: records ACK, processes queue
- Thread-safe using `synchronized`

#### 4.3 Queue Processing

```java
private void processQueue()
```

- Checks if front request has all ACKs
- If yes, executes request and removes from queue
- Continues until queue is empty or waiting for ACKs

#### 4.4 File Operation Methods

```java
public void uploadFile(String fileName, byte[] fileData)
public void deleteFile(String fileName)
public void downloadFile(String fileName)
public void searchFile(String fileName)
```

- Create timestamped request
- Update logical clock
- Multicast request to all nodes
- Execution happens automatically when ACKs received

### 5. Testing and Demonstration

#### 5.1 Test Scenario: Concurrent Upload and Delete

**Scenario**:

- Node A uploads file "test.txt"
- Node B concurrently deletes "test.txt"

**Expected Behavior**:

- All nodes receive both requests
- Requests are ordered consistently (by timestamp)
- All nodes execute in same order
- Final state is consistent across all nodes

**How to Test**:

1. Start all 4 nodes
2. On Node1: `upload test.txt ./test.txt`
3. On Node2 (concurrently): `delete test.txt`
4. Observe logs showing consistent ordering

#### 5.2 Screenshots and Output

The system provides detailed logging:

- Request multicast notifications
- ACK receipts
- Request execution
- Logical clock updates
- Leader information

### 6. Assumptions

- Multicasts also go to sender (self-delivery)
- All messages are eventually delivered (no message loss)
- Node failures not considered (crashed node stops system)
- All nodes know about all other nodes at startup

### 7. Challenges and Solutions

**Challenge 1**: Ensuring total ordering

- **Solution**: Priority queue with timestamp + messageId ordering

**Challenge 2**: Thread safety

- **Solution**: `synchronized` methods and concurrent collections

**Challenge 3**: RMI stub management

- **Solution**: Lazy initialization with caching

**Challenge 4**: Leader election

- **Solution**: Random selection (basic) or Raft (bonus)

### 8. Conclusion

The distributed file system successfully implements:

- ✅ TO-Multicasting with logical clocks
- ✅ Four file operations (Upload, Download, Delete, Search)
- ✅ Consistent state across all nodes
- ✅ Leader election (basic + Raft bonus)

The system demonstrates how TO-Multicasting ensures consistency even with concurrent operations, preventing the scenario where different nodes end up in different states.

### 9. Future Improvements

- Handle node failures and recovery
- Implement proper Raft RPCs (RequestVote, AppendEntries)
- Add file transfer for Download operations
- Implement message persistence
- Add network partition handling
