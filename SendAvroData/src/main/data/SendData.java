package main.data;

import java.util.Map;

/**
 * Created by sunyulong on 16.7.14.
 *
 */
public interface SendData {
    public long sendData(String name,String data);
    public long sendData(Map<String,String> data);
}
