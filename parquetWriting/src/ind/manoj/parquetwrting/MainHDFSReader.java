package ind.manoj.parquetwrting;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.thrift.ThriftParquetReader;

import java.io.File;
import java.io.IOException;

public class MainHDFSReader {
    public static void main(String[] args) throws IOException {
        String inFile = "hdfs://localhost:9000/parquet/hello.par";
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");

        Path path = new Path(inFile);

        ThriftParquetReader.Builder<Msg> msgBuilder = ThriftParquetReader.build(path);
        ParquetReader<Msg> msgParquetReader = msgBuilder.withConf(conf).build();

        Msg readMsg = msgParquetReader.read();

        while (readMsg != null){
            System.out.println(readMsg.getName() + "----" +readMsg.getAge());
            readMsg = msgParquetReader.read();
        }
    }
}
