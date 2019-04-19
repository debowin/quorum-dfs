import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

// Generated code
public class Node {
    static NodeHandler handler;
    private static NodeService.Processor<NodeHandler> processor;
    static Properties prop;

    public static void main(String[] args) {
        try {
            prop = new Properties();
            InputStream is = new FileInputStream("simpledfs.cfg");
            prop.load(is);

            // read node id from cli
            Integer nodeIndex = Integer.valueOf(args[0]);
            handler = new NodeHandler(prop, nodeIndex);
            processor = new NodeService.Processor<>(handler);

            startThreadPoolServer(nodeIndex);
        } catch (Exception x) {
            x.printStackTrace();
        }
    }

    private static void startThreadPoolServer(Integer nodeIndex) {
        try {
            int serverPort = Integer.parseInt(prop.getProperty("node.ports").split("\\s*,\\s*")[nodeIndex]);
            // Create Thrift server socket as a thread pool
            TServerTransport serverTransport = new TServerSocket(serverPort);
            TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverTransport);
            args.processor(processor);
            TServer server = new TThreadPoolServer(args);

            System.out.printf("Starting the DHTNode(No. %d)...\n", nodeIndex);
            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

