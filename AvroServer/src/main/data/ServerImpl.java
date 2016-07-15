package main.data;

import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Properties;

/**
 * Created by sunyulong on 16.7.15.
 *
 */
public class ServerImpl extends ServerProperties implements Server{
    private static Logger logger = Logger.getLogger(ServerImpl.class);
    private static String host = null;
    private static int port = 0;
    static {
        try {
            if (logger.isDebugEnabled())
                logger.debug("starting to load config file...");
            InputStream is = ClassLoader.getSystemResourceAsStream("AvroServer.properties");
            Properties properties = new Properties();
            properties.load(is);
            host = properties.getProperty(HOST);
            port = Integer.parseInt(properties.getProperty(PORT));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void run() {
        try {
            new NettyServer(new SpecificResponder(messageProtocol.class,
                    new ReceiveData()), new InetSocketAddress(port));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
