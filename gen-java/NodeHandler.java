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
        // TODO
        System.out.println("Filename to Replica Mapping:");
        for (Map.Entry<String, Path> nameReplica : nameReplicaMap.entrySet()) {
            System.out.printf("%s ::: %s\n", nameReplica.getKey(), nameReplica.getValue().toString());
        }
        // TODO
        System.out.println("Filename to Version Mapping:");
        for (Map.Entry<String, Integer> nameVersion : nameVersionMap.entrySet()) {
            System.out.printf("%s ::: %s\n", nameVersion.getKey(), nameVersion.getValue());
        }

        // coordinator specifics
        if (thisNode.equals(coordNode)) {
            int nodesCount = Integer.parseInt(prop.getProperty("nodes.count"));
            assert addresses.length == nodesCount;
            assert ports.length == nodesCount;
            nodes = new ArrayList<>();
            for (int i = 0; i < nodesCount; i++) {
                nodes.add(new Node(addresses[i], Integer.parseInt(ports[i])));
            }
            // TODO
            System.out.println("DFS Node Addresses and Ports:");
            for (Node node : nodes) {
                System.out.printf("%s ::: %s\n", node.address, node.port);
            }
            readQuorumSize = Integer.parseInt(prop.getProperty("quorum.read"));
            writeQuorumSize = Integer.parseInt(prop.getProperty("quorum.write"));
            nameLockMap = new HashMap<>();
            for (String name : nameVersionMap.keySet()) {
                nameLockMap.put(name, new ReentrantLock());
            }
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
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean write(String filename, String content) throws TException {
        System.out.printf("Write(%s, %s) Called.\n", filename, content);
        boolean result;
        if (coordNode.equals(thisNode)) {
            // if this is the coordinator
            result = coordWrite(filename, content);
        } else {
            TTransport transport = new TSocket(coordNode.address, coordNode.port);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            NodeService.Client client = new NodeService.Client(protocol);
            result = client.coordWrite(filename, content);
            transport.close();
        }
        System.out.printf("Write(%s, %s) Returning.\n", filename, content);
        return result;
    }

    @Override
    public String read(String filename) throws TException {
        try {
            System.out.printf("Read(%s) Called.\n", filename);
            String content;
            if (coordNode.equals(thisNode)) {
                // if this is the coordinator
                content = coordRead(filename);
            } else {
                TTransport transport = new TSocket(coordNode.address, coordNode.port);
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
                NodeService.Client client = new NodeService.Client(protocol);
                content = client.coordRead(filename);
                transport.close();
            }
            System.out.printf("Read(%s) Returning.\n", filename);
            return content;
        } catch (TException e) {
            e.printStackTrace();
            throw new TException("FileNotFound");
        }
    }

    @Override
    public boolean quorumWrite(String fileName, String content, int version) throws TException {
        try {
            String fileContent = version + "\n" + content;
            Path replica;
            int currentVersion = nameVersionMap.get(fileName) != null ? nameVersionMap.get(fileName) : 0;
            if (currentVersion >= version) {
                // if already on a newer or same version, skip write
                return true;
            } else if (currentVersion == 0) {
                // if this is the first version of the file
                replica = Paths.get(replicaPath, fileName);
                nameReplicaMap.put(fileName, replica);
            } else {
                // if file is already known
                replica = nameReplicaMap.get(fileName);
            }
            Files.write(replica, fileContent.getBytes());
            nameVersionMap.put(fileName, version);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public String quorumRead(String filename) throws TException {
        try {
            Path replica = nameReplicaMap.get(filename);
            List<String> content = Files.readAllLines(replica);
            // remove version line
            content = content.subList(1, content.size());
            return String.join("\n", content);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * return version if file exists, 0 if not.
     *
     * @param filename
     * @return
     * @throws TException
     */
    @Override
    public int quorumVersion(String filename) throws TException {
        return nameVersionMap.get(filename) != null ? nameVersionMap.get(filename) : 0;
    }

    @Override
    public boolean coordWrite(String fileName, String content) throws TException {
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
            // get the node with latest version
            int maxVersion = 0;
            int version;
            for (Node node : quorum) {
                if (node.equals(thisNode)) {
                    // if this is the coordinator
                    version = quorumVersion(fileName);
                } else {
                    TTransport transport = new TSocket(coordNode.address, coordNode.port);
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
                    TTransport transport = new TSocket(coordNode.address, coordNode.port);
                    transport.open();
                    TProtocol protocol = new TBinaryProtocol(transport);
                    NodeService.Client client = new NodeService.Client(protocol);
                    result = client.quorumWrite(fileName, content, maxVersion + 1);
                    transport.close();
                }
            }
            return result;
        } finally {
            replicaLock.unlock();
        }
    }

    @Override
    public String coordRead(String filename) throws TException {
        List<Node> quorum = createQuorum(readQuorumSize);
        // get the node with latest version
        int maxVersion = 0;
        Node latestNode = quorum.get(0);
        int version;
        for (Node node : quorum) {
            if (node.equals(thisNode)) {
                // if this is the coordinator
                version = quorumVersion(filename);
            } else {
                TTransport transport = new TSocket(coordNode.address, coordNode.port);
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
                NodeService.Client client = new NodeService.Client(protocol);
                version = client.quorumVersion(filename);
                transport.close();
            }
            if (version > maxVersion) {
                maxVersion = version;
                latestNode = node;
            }
        }

        if (maxVersion == 0) {
            // if the file doesn't absolutely exist
            return null;
        }

        // get the contents of the latest file
        String content;
        if (latestNode.equals(thisNode)) {
            // if this is the coordinator
            content = quorumRead(filename);
        } else {
            TTransport transport = new TSocket(coordNode.address, coordNode.port);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            NodeService.Client client = new NodeService.Client(protocol);
            content = client.quorumRead(filename);
            transport.close();
        }
        return content;
    }

    private List<Node> createQuorum(int quorumSize) {
        List<Node> quorum = new ArrayList<>(nodes);
        Collections.shuffle(quorum);
        return quorum.subList(0, quorumSize);
    }
}