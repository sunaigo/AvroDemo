import main.data.SendData;
import main.data.SendDataImpl;

import java.io.*;

/**
 * Created by sunyulong on 16.7.15.
 *
 */
public class SendDemo {
    public static void main(String [] args){
        SendData send = new SendDataImpl();
        send.sendData("张飞","乌干达");
    }
}
