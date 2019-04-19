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

public class NodeHandler implements NodeService.Iface {
    Properties prop;
    String address;
    int port;
    String replicaPath;
    Map<String, File> replicas;

    NodeHandler(Properties properties, int nodeIndex) {
        prop = properties;
        address = prop.getProperty("node.addresses").split("\\s*,\\s*")[nodeIndex];
        port = Integer.valueOf(prop.getProperty("node.ports").split("\\s*,\\s*")[nodeIndex]);
        replicaPath = prop.getProperty("tempdir.prefix") + nodeIndex;
        replicas = new HashMap<>();
        createLocalReplica(prop.getProperty("fs.path"));
        // TODO debug statement
        for (Map.Entry<String, File> replica : replicas.entrySet()) {
            System.out.printf("%s ::: %s\n", replica.getKey(), replica.getValue().getAbsolutePath());
        }
    }

    private void createLocalReplica(String srcPath) {
        try {
            File src = new File(srcPath);
            for(File original: Objects.requireNonNull(src.listFiles())){
                Path replica = Paths.get(replicaPath, original.getName());
                Files.copy(original.toPath(), replica, StandardCopyOption.REPLACE_EXISTING);
                replicas.put(original.getName(), replica.toFile());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean write(String filename, String content) throws TException {
        return false;
    }

    @Override
    public String read(String filename) throws TException {
        return null;
    }

    @Override
    public boolean quorumWrite(String filename, String content) throws TException {
        return false;
    }

    @Override
    public String quorumRead(String filename) throws TException {
        return null;
    }

    @Override
    public int quorumVersion(String filename) throws TException {
        return 0;
    }

    @Override
    public boolean coordWrite(String filename, String content) throws TException {
        return false;
    }

    @Override
    public String coordRead(String filename) throws TException {
        return null;
    }
}