package main.data;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.lang.time.DateUtils;

import java.io.File;
import java.io.IOException;
import java.util.Date;

/**
 * Created by sunyulong on 16.7.15.
 *
 */
public class ReceiveData implements messageProtocol {
    @Override
    public message sendMessage(message message) throws AvroRemoteException {
        File file = new File("message.avro");
        DatumWriter<message> userDatumWriter = new SpecificDatumWriter<>(message.class);
        DataFileWriter<message> dataFileWriter = new DataFileWriter<>(userDatumWriter);
        try {
            //如果文件不存在就创建，如果存在就在文件末尾进行追加
            if(!file.exists()){
                dataFileWriter.create(message.getSchema(),file);
            }else{
                dataFileWriter.appendTo(file);
            }
            dataFileWriter.append(message);
            dataFileWriter.close();
            String name = (String) message.getName();
            String content = (String) message.getContent();
            long time = message.getTimestmp();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return message;
    }
}
