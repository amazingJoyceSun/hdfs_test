package hdfs.write;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class HdfsWrite {
    /*
    joyce,HdfsWrite demo
    2022/10/10
    */
    public static void main(String args[]) throws IOException {
        InputStream input;
        OutputStream output;

        Configuration conf = new Configuration();

        conf.set("fs.defaultFS","hdfs://172.16.120.27:8020");
        FileSystem fs = FileSystem.get(conf);

       output =  fs.create(new Path("/test/upload.txt"));
       input = new FileInputStream("/Users/joyce/joycefile/2.txt");

        byte[] bytes = new byte[1024];
        int len = 0;
        while((len = input.read(bytes))!=-1){
            output.write(bytes,0,len);
        }

        input.close();
        output.flush();
        output.close();
    }
}