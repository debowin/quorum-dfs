import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.Console;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class Client {
    enum Option {
        read, write, ls, simRead, simWrite, exit
    }

    private static Properties prop;
    private static List<NodeHandler.Node> nodes;

    public static void main(String[] args) {
        try {
            // Get Configs
            prop = new Properties();
            InputStream is = new FileInputStream("simpledfs.cfg");
            prop.load(is);

            // get all nodes info
            String[] addresses = prop.getProperty("node.addresses").split("\\s*,\\s*");
            String[] ports = prop.getProperty("node.ports").split("\\s*,\\s*");

            int nodesCount = Integer.parseInt(prop.getProperty("nodes.count"));

            assert addresses.length == nodesCount;
            assert ports.length == nodesCount;

            nodes = new ArrayList<>();
            for (int i = 0; i < nodesCount; i++) {
                nodes.add(new NodeHandler.Node(addresses[i], Integer.parseInt(ports[i])));
            }

            Console console = System.console();
            NodeHandler.Node node;
            Option option = Option.ls;
            while (option != Option.exit) {
                // UI Menu Loop
                option = Option.valueOf(console.readLine("CHOOSE> read, write, ls, simRead, simWrite, exit\n> "));
                switch (option) {
                    case read:
                        String fileName = console.readLine("Enter File Name: ");
                        node = getRandomNode();
                        read(fileName, node);
                        break;
                    case write:
                        String[] fileNameContents = console.readLine("Enter File Name, Contents: ").split("\\s*,\\s*");
                        node = getRandomNode();
                        write(fileNameContents[0], fileNameContents[1], node);
                        break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void read(String fileName, NodeHandler.Node node){
        try {
            // create client connection
            System.out.printf("Sending Read(%s) to %s:%d\n", fileName, node.address, node.port);
            TTransport transport = new TSocket(node.address, node.port);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            NodeService.Client client = new NodeService.Client(protocol);
            String contents = client.read(fileName);
            System.out.printf("Read(%s) Successful!\n=== CONTENT BEGINS ===\n%s\n=== CONTENT ENDS ===\n",
                    fileName, contents);
            transport.close();
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    private static void write(String fileName, String contents, NodeHandler.Node node){
        try {
            // create client connection
            System.out.printf("Sending Write(%s, %s) to %s:%d\n", fileName, contents, node.address, node.port);
            TTransport transport = new TSocket(node.address, node.port);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            NodeService.Client client = new NodeService.Client(protocol);
            boolean result = client.write(fileName, contents);
            if (!result) {
                System.out.printf("Write(%s, %s) Failed!\n", fileName, contents);
            } else {
                System.out.printf("Write(%s, %s) Successful!\n", fileName, contents);
            }
            transport.close();
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    private static NodeHandler.Node getRandomNode() {
        Random random = new Random();
        int nodeIndex = random.nextInt(nodes.size());
        return nodes.get(nodeIndex);
    }
}
