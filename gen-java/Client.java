import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.Console;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

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
                    case ls:
                        node = getRandomNode();
                        ls(node);
                        break;
                    case simRead:
                        simRead();
                        break;
                    case simWrite:
                        simWrite();
                        break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static int read(String fileName, NodeHandler.Node node) {
        try {
            // create client connection
            System.out.printf("read(%s) -> %s:%d\n", fileName, node.address, node.port);
            TTransport transport = new TSocket(node.address, node.port);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            NodeService.Client client = new NodeService.Client(protocol);
            Instant start = Instant.now();
            String contents = client.read(fileName);
            Instant end = Instant.now();
            int time =  (int)Duration.between(start, end).toMillis();
            System.out.printf("read(%s) completed in %d ms!\n=== CONTENT BEGINS ===\n%s\n=== CONTENT ENDS ===\n",
                    fileName, time, contents);
            transport.close();
            return time;
        } catch (DFSError e) {
            System.out.println(e.toString());
        } catch (TException e) {
            e.printStackTrace();
        }
        return 0;
    }

    private static int write(String fileName, String contents, NodeHandler.Node node) {
        try {
            // create client connection
            System.out.printf("write(%s) -> %s:%d\n", fileName, node.address, node.port);
            TTransport transport = new TSocket(node.address, node.port);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            NodeService.Client client = new NodeService.Client(protocol);
            Instant start = Instant.now();
            boolean result = client.write(fileName, contents);
            Instant end = Instant.now();
            int time = (int)Duration.between(start, end).toMillis();
            if (result) {
                System.out.printf("File %s not found on DFS so created with Version 1.\n", fileName);
            }
            System.out.printf("write(%s) completed in %d ms!\n", fileName, time);
            transport.close();
            return time;
        } catch (DFSError e) {
            System.out.println(e.toString());
        } catch (TException e) {
            e.printStackTrace();
        }
        return 0;
    }

    private static void ls(NodeHandler.Node node) {
        try {
            // create client connection
            System.out.printf("ls() -> %s:%d\n", node.address, node.port);
            TTransport transport = new TSocket(node.address, node.port);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            NodeService.Client client = new NodeService.Client(protocol);
            Map<String, Integer> lsResult = client.ls();
            System.out.printf("ls() successful!\n%20s\t\tVersion\n", "FileName");
            for (Map.Entry<String, Integer> lsRecord : lsResult.entrySet()) {
                System.out.printf("%20s\t\t%d\n", lsRecord.getKey(), lsRecord.getValue());
            }
            transport.close();
        } catch (DFSError e) {
            System.out.println(e.toString());
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    private static NodeHandler.Node getRandomNode() {
        Random random = new Random();
        int nodeIndex = random.nextInt(nodes.size());
        return nodes.get(nodeIndex);
    }

    private static void simRead() {
        int timeSum = 0;
        int timeCount = 0;
        try {
            int maxDelay = Integer.parseInt(prop.getProperty("client.maxdelay"));
            Random random = new Random();
            Path fnFile = Paths.get(prop.getProperty("client.filenamesListFile"));
            List<String> fnList = Files.readAllLines(fnFile);
            while (true) {
                Thread.sleep((long) (maxDelay * random.nextFloat()));
                NodeHandler.Node node = getRandomNode();
                String fileName = fnList.get(random.nextInt(fnList.size()));
                timeSum += read(fileName, node);
                timeCount++;
                System.out.printf("Average Runtime: %.2f ms\n", (float)timeSum/timeCount);
            }
        } catch (IOException e) {
            System.out.printf("Filenames List File doesn't exist at %s.\n",
                    prop.getProperty("client.filenamesListFile"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void simWrite() {
        int timeSum = 0;
        int timeCount = 0;
        try {
            int maxDelay = Integer.parseInt(prop.getProperty("client.maxdelay"));
            Random random = new Random();
            Path fnFile = Paths.get(prop.getProperty("client.filenamesListFile"));
            List<String> fnList = Files.readAllLines(fnFile);
            Path contentsFile = Paths.get(prop.getProperty("client.contentsListFile"));
            List<String> contentsList = Files.readAllLines(contentsFile);
            while (true) {
                Thread.sleep((long) (maxDelay * random.nextFloat()));
                NodeHandler.Node node = getRandomNode();
                String fileName = fnList.get(random.nextInt(fnList.size()));
                String content = contentsList.get(random.nextInt(contentsList.size()));
                timeSum += write(fileName, content, node);
                timeCount++;
                System.out.printf("Average Runtime: %.2f ms\n", (float)timeSum/timeCount);
            }
        } catch (IOException e) {
            System.out.printf("Filenames or Contents List File doesn't exist at %s or %s respectively.\n",
                    prop.getProperty("client.filenamesListFile"), prop.getProperty("client.contentsListFile"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
