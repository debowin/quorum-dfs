import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class NodeHandler implements NodeService.Iface {
    Properties prop;
    Node thisNode;
    String replicaPath;
    Map<String, Path> nameReplicaMap;
    Map<String, Integer> nameVersionMap;
    Map<String, Lock> nameLockMap;
    List<Node> nodes;
    Node coordNode;
    int readQuorumSize;
    int writeQuorumSize;

    static class Node {
        String address;
        int port;

        public Node(String address, int port) {
            this.address = address;
            this.port = port;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || obj.getClass() != this.getClass()) {
                return false;
            }
            Node node = (Node) obj;
            return this.address.equals(node.address) && this.port == node.port;
        }
    }

    NodeHandler(Properties properties, int nodeIndex) {
        prop = properties;

        // get current node and coordinator node info
        String[] addresses = prop.getProperty("node.addresses").split("\\s*,\\s*");
        String[] ports = prop.getProperty("node.ports").split("\\s*,\\s*");
        thisNode = new Node(addresses[nodeIndex], Integer.parseInt(ports[nodeIndex]));
        int coordIndex = Integer.parseInt(prop.getProperty("coord.id"));
        coordNode = new Node(addresses[coordIndex], Integer.parseInt(ports[coordIndex]));

        // local replica stuff
        replicaPath = prop.getProperty("tempdir.prefix") + nodeIndex;
        nameReplicaMap = new HashMap<>();
        nameVersionMap = new HashMap<>();
        createLocalReplica(prop.getProperty("fs.path"));
        System.out.println("Filename to Replica Mapping:");
        for (Map.Entry<String, Path> nameReplica : nameReplicaMap.entrySet()) {
            System.out.printf("%s ::: %s\n", nameReplica.getKey(), nameReplica.getValue().toString());
        }
        System.out.println("Filename to Version Mapping:");
        for (Map.Entry<String, Integer> nameVersion : nameVersionMap.entrySet()) {
            System.out.printf("%s ::: %s\n", nameVersion.getKey(), nameVersion.getValue());
        }

        // other nodes stuff
        int nodesCount = Integer.parseInt(prop.getProperty("nodes.count"));
        assert addresses.length == nodesCount;
        assert ports.length == nodesCount;
        nodes = new ArrayList<>();
        for (int i = 0; i < nodesCount; i++) {
            nodes.add(new Node(addresses[i], Integer.parseInt(ports[i])));
        }
        System.out.println("DFS Node Addresses and Ports:");
        for (Node node : nodes) {
            System.out.printf("%s ::: %s\n", node.address, node.port);
        }

        // coordinator specifics
        if (thisNode.equals(coordNode)) {
            readQuorumSize = Integer.parseInt(prop.getProperty("quorum.read"));
            writeQuorumSize = Integer.parseInt(prop.getProperty("quorum.write"));
            nameLockMap = new HashMap<>();
            for (String name : nameVersionMap.keySet()) {
                nameLockMap.put(name, new ReentrantLock());
            }
            // launch synchronization thread in the background
            new Thread(this::synchronize).start();
        }
    }

    private void createLocalReplica(String srcPath) {
        try {
            File src = new File(srcPath);
            Files.createDirectories(Paths.get(replicaPath));
            for (File original : Objects.requireNonNull(src.listFiles())) {
                Path replica = Paths.get(replicaPath, original.getName());
                Files.copy(original.toPath(), replica, StandardCopyOption.REPLACE_EXISTING);
                nameReplicaMap.put(original.getName(), replica);
                nameVersionMap.put(original.getName(), 1);
            }
        } catch (Exception e) {
            System.out.println("Error in Creating Local Replica.");
            e.printStackTrace();
            System.exit(-1);
        }
    }

    @Override
    public boolean write(String fileName, String content) throws TException {
        // pass on this call to the coordinator and relay the result back
        System.out.printf("write(%s) invoked.\n", fileName);
        boolean result;
        if (coordNode.equals(thisNode)) {
            // if this is the coordinator
            result = coordWrite(fileName, content);
        } else {
            TTransport transport = new TSocket(coordNode.address, coordNode.port);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            NodeService.Client client = new NodeService.Client(protocol);
            result = client.coordWrite(fileName, content);
            transport.close();
        }
        return result;
    }

    @Override
    public String read(String fileName) throws TException {
        // pass on this call to the coordinator and relay the result back
        System.out.printf("read(%s) called.\n", fileName);
        String content;
        if (coordNode.equals(thisNode)) {
            // if this is the coordinator
            content = coordRead(fileName);
        } else {
            TTransport transport = new TSocket(coordNode.address, coordNode.port);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            NodeService.Client client = new NodeService.Client(protocol);
            content = client.coordRead(fileName);
            transport.close();
        }
        return content;
    }

    @Override
    public Map<String, Integer> ls() throws TException {
        // pass on this call to the coordinator and relay the result back
        System.out.println("ls() called.");
        Map<String, Integer> lsResult;
        if (coordNode.equals(thisNode)) {
            // if this is the coordinator
            lsResult = coordLS();
        } else {
            TTransport transport = new TSocket(coordNode.address, coordNode.port);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            NodeService.Client client = new NodeService.Client(protocol);
            lsResult = client.coordLS();
            transport.close();
        }
        return lsResult;
    }

    @Override
    public boolean quorumWrite(String fileName, String content, int version) throws TException {
        System.out.printf("quorumWrite(%s, %d) called.\n", fileName, version);
        boolean fileCreated = false;
        try {
            String fileContent = version + "\n" + content;
            Path replica;
            int currentVersion = nameVersionMap.get(fileName) != null ? nameVersionMap.get(fileName) : 0;
            if (currentVersion >= version) {
                // if already on a newer or same version, skip write
                System.out.printf("File %s already on same or newer version, skip write.\n", fileName);
                return fileCreated;
            } else if (currentVersion == 0) {
                // if this is the first version of the file
                replica = Paths.get(replicaPath, fileName);
                nameReplicaMap.put(fileName, replica);
                fileCreated = true;
            } else {
                // if file is already known
                replica = nameReplicaMap.get(fileName);
            }
            Files.write(replica, fileContent.getBytes());
            nameVersionMap.put(fileName, version);
            return fileCreated;
        } catch (IOException e) {
            e.printStackTrace();
            throw new DFSError(String.format("Error writing to File %s Version %d", fileName, version));
        }
    }

    @Override
    public String quorumRead(String fileName) throws TException {
        System.out.printf("quorumRead(%s) called.\n", fileName);
        try {
            Path replica = nameReplicaMap.get(fileName);
            List<String> content = Files.readAllLines(replica);
            // remove version line
            content = content.subList(1, content.size());
            return String.join("\n", content);
        } catch (IOException e) {
            e.printStackTrace();
            throw new DFSError(String.format("Error reading from File %s", fileName));
        }
    }

    /**
     * return version if file exists, 0 if not.
     *
     * @param fileName
     * @return
     * @throws TException
     */
    @Override
    public int quorumVersion(String fileName) throws TException {
        return nameVersionMap.get(fileName) != null ? nameVersionMap.get(fileName) : 0;
    }

    @Override
    public boolean coordWrite(String fileName, String content) throws TException {
        System.out.printf("coordWrite(%s) called.\n", fileName);
        Lock replicaLock = nameLockMap.get(fileName);
        if (replicaLock == null) {
            // file doesn't exist already
            nameVersionMap.put(fileName, 0);
            nameLockMap.put(fileName, new ReentrantLock());
            replicaLock = nameLockMap.get(fileName);
        }
        replicaLock.lock();
        try {
            List<Node> quorum = createQuorum(writeQuorumSize);
            // get the latest version
            int maxVersion = 0;
            int version;
            for (Node node : quorum) {
                if (node.equals(thisNode)) {
                    // if this is the coordinator
                    version = quorumVersion(fileName);
                } else {
                    TTransport transport = new TSocket(node.address, node.port);
                    transport.open();
                    TProtocol protocol = new TBinaryProtocol(transport);
                    NodeService.Client client = new NodeService.Client(protocol);
                    version = client.quorumVersion(fileName);
                    transport.close();
                }
                if (version > maxVersion) {
                    maxVersion = version;
                }
            }

            // write the new content to everyone in the quorum
            boolean result = false;
            for (Node node : quorum) {
                if (node.equals(thisNode)) {
                    result = quorumWrite(fileName, content, maxVersion + 1);
                } else {
                    TTransport transport = new TSocket(node.address, node.port);
                    transport.open();
                    TProtocol protocol = new TBinaryProtocol(transport);
                    NodeService.Client client = new NodeService.Client(protocol);
                    result = client.quorumWrite(fileName, content, maxVersion + 1);
                    transport.close();
                }
            }
            return result;
        } catch (DFSError e) {
            throw e;
        } catch (TException e) {
            e.printStackTrace();
            throw new DFSError("DFS Node Failure.");
        } finally {
            replicaLock.unlock();
        }
    }

    @Override
    public String coordRead(String fileName) throws TException {
        System.out.printf("coordRead(%s) called.\n", fileName);
        List<Node> quorum = createQuorum(readQuorumSize);
        // get the node with latest version
        int maxVersion = 0;
        Node latestNode = quorum.get(0);
        int version;
        try {
            for (Node node : quorum) {
                if (node.equals(thisNode)) {
                    // if this is the coordinator
                    version = quorumVersion(fileName);
                } else {
                    TTransport transport = new TSocket(node.address, node.port);
                    transport.open();
                    TProtocol protocol = new TBinaryProtocol(transport);
                    NodeService.Client client = new NodeService.Client(protocol);
                    version = client.quorumVersion(fileName);
                    transport.close();
                }
                if (version > maxVersion) {
                    maxVersion = version;
                    latestNode = node;
                }
            }
        } catch (DFSError e) {
            throw e;
        } catch (TException e) {
            e.printStackTrace();
            throw new DFSError("DFS Node Failure.");
        }

        if (maxVersion == 0) {
            // if the file doesn't absolutely exist
            throw new DFSError(String.format("File %s doesn't exist in the DFS. Run 'ls' to see the list of files.", fileName));
        }

        // get the contents of the latest file
        try {
            String content;
            if (latestNode.equals(thisNode)) {
                // if this is the coordinator
                content = quorumRead(fileName);
            } else {
                TTransport transport = new TSocket(latestNode.address, latestNode.port);
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
                NodeService.Client client = new NodeService.Client(protocol);
                content = client.quorumRead(fileName);
                transport.close();
            }
            return content;
        } catch (DFSError e) {
            throw e;
        } catch (TException e) {
            e.printStackTrace();
            throw new DFSError("DFS Node Failure.");
        }
    }

    @Override
    public Map<String, Integer> coordLS() throws TException {
        /* using a read quorum - contacting every node would be inefficient, and we know
           a read quorum is guaranteed to have the latest version of any file in the DFS. */
        try {
            System.out.println("coordLS() called.");
            List<Node> quorum = createQuorum(readQuorumSize);
            Map<String, Integer> lsResult = new HashMap<>(nameVersionMap);
            // get the latest version of each file
            for (String fileName : nameVersionMap.keySet()) {
                int maxVersion = 0;
                int version;
                for (Node node : quorum) {
                    if (node.equals(thisNode)) {
                        // if this is the coordinator
                        version = quorumVersion(fileName);
                    } else {
                        TTransport transport = new TSocket(node.address, node.port);
                        transport.open();
                        TProtocol protocol = new TBinaryProtocol(transport);
                        NodeService.Client client = new NodeService.Client(protocol);
                        version = client.quorumVersion(fileName);
                        transport.close();
                    }
                    if (version > maxVersion) {
                        maxVersion = version;
                    }
                }
                lsResult.put(fileName, maxVersion);
            }
            return lsResult;
        } catch (DFSError e) {
            throw e;
        } catch (TException e) {
            throw new DFSError("DFS Node Failure.");
        }
    }

    private List<Node> createQuorum(int quorumSize) {
        List<Node> quorum = new ArrayList<>(nodes);
        Collections.shuffle(quorum);
        return quorum.subList(0, quorumSize);
    }

    private void synchronize() {
        /* using a read quorum - contacting every node would be inefficient, and we know
           a read quorum is guaranteed to have the latest version of any file in the DFS. */
        while (true) {
            try {
                // run till this program runs, sleeping for a delay after each run
                Thread.sleep(Integer.parseInt(prop.getProperty("node.syncdelay")));
                List<Node> quorum = createQuorum(readQuorumSize);
                System.out.println("synchronize() begins...");
                for (String fileName : nameVersionMap.keySet()) {
                    // all writes must be sequential - even synchronize.
                    Lock replicaLock = nameLockMap.get(fileName);
                    replicaLock.lock();
                    try {
                        // find the node which has the latest version of this file in the read quorum
                        int maxVersion = 0;
                        int version;
                        Node latestNode = quorum.get(0);
                        Map<Node, Integer> nodeVersionMapping = new HashMap<>();
                        for (Node node : quorum) {
                            if (node.equals(thisNode)) {
                                // if this is the same node
                                version = quorumVersion(fileName);
                            } else {
                                TTransport transport = new TSocket(node.address, node.port);
                                transport.open();
                                TProtocol protocol = new TBinaryProtocol(transport);
                                NodeService.Client client = new NodeService.Client(protocol);
                                version = client.quorumVersion(fileName);
                                transport.close();
                            }
                            nodeVersionMapping.put(node, version);
                            if (version > maxVersion) {
                                maxVersion = version;
                                latestNode = node;
                            }
                        }

                        // get the contents of the latest file
                        String latestContent;
                        if (latestNode.equals(thisNode)) {
                            // if this is the same node
                            latestContent = quorumRead(fileName);
                        } else {
                            TTransport transport = new TSocket(latestNode.address, latestNode.port);
                            transport.open();
                            TProtocol protocol = new TBinaryProtocol(transport);
                            NodeService.Client client = new NodeService.Client(protocol);
                            latestContent = client.quorumRead(fileName);
                            transport.close();
                        }

                        // write this latest content to every node that is stale
                        for (Node node : nodes) {
                            if (!nodeVersionMapping.containsKey(node) || nodeVersionMapping.get(node) < maxVersion) {
                                if (node == thisNode) {
                                    // if this is the same node
                                    quorumWrite(fileName, latestContent, maxVersion);
                                } else {
                                    TTransport transport = new TSocket(node.address, node.port);
                                    transport.open();
                                    TProtocol protocol = new TBinaryProtocol(transport);
                                    NodeService.Client client = new NodeService.Client(protocol);
                                    client.quorumWrite(fileName, latestContent, maxVersion);
                                    transport.close();
                                }
                            }
                        }
                    } finally {
                        replicaLock.unlock();
                    }
                }
                System.out.println("synchronize() ends...");
            } catch (InterruptedException | TException e) {
                System.out.println("synchronize() failed.");
                e.printStackTrace();
            }
        }
    }
}