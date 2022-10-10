package hdfs.read;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsRead {
    /*
    joyce,HdfsRead demo
    2022/10/10
    */
    public static void main(String args[]) throws Exception {
        InputStream input;
        OutputStream output;

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://172.16.120.27:8020");//rpc连接端口
        try {
            FileSystem fs = FileSystem.get(conf);

            output = new FileOutputStream("/Users/joyce/joycefile/1.txt");

            input = fs.open(new Path("/test/20221010.txt"));
            byte[] bytes = new byte[1024];
            int len = 0;
            while((len = input.read(bytes))!=-1){
                output.write(bytes,0,len);
            }

            input.close();
            output.flush();
            output.close();
        }catch(IOException ie){
            ie.printStackTrace();
        }
    }
}