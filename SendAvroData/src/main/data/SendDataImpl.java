package main.data;

import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
/**
 * Created by sunyulong on 16.7.15.
 *
 */

public class SendDataImpl extends ClientProperties implements SendData {
    private static Logger logger = Logger.getLogger(SendDataImpl.class);
    private static String host = null;
    private static int port = 0;
    static {
        try {
            if (logger.isDebugEnabled())
                logger.debug("starting to load config file...");
            InputStream is = ClassLoader.getSystemResourceAsStream("AvroClient.properties");
            Properties properties = new Properties();
            properties.load(is);
            host = properties.getProperty(HOST);
            port = Integer.parseInt(properties.getProperty(PORT));
            } catch (IOException e) {
                e.printStackTrace();
        }
    }


    @Override
    public long sendData(String name, String data) {
        //创建客户端
        NettyTransceiver client;
        long start = 0;
        long end = 0;
        try {
            client = new NettyTransceiver(new InetSocketAddress(host, port));
            messageProtocol proxy = SpecificRequestor.getClient(messageProtocol.class, client);
            message message = new message(name,data, System.currentTimeMillis());
            start = System.currentTimeMillis();
            proxy.sendMessage(message);
            end = System.currentTimeMillis();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return end - start;
    }

    @Override
    public long sendData(Map<String,String> data) {
        NettyTransceiver client;
        long start = 0;
        long end = 0;
        try {
            client = new NettyTransceiver(new InetSocketAddress(host, port));
            messageProtocol proxy = SpecificRequestor.getClient(messageProtocol.class, client);
            Iterator<Map.Entry<String, String>> iterator = data.entrySet().iterator();
            start = System.currentTimeMillis();
            while(iterator.hasNext()){
                Map.Entry<String, String> kv = iterator.next();
                message message = new message(kv.getKey(),kv.getValue(),System.currentTimeMillis());
                proxy.sendMessage(message);
            }
            end = System.currentTimeMillis();
         } catch (IOException e) {
           e.printStackTrace();
         }
        return end - start;
    }
}
