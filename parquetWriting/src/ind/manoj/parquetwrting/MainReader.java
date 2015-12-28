package ind.manoj.parquetwrting;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.thrift.ThriftParquetReader;
import org.apache.thrift.TBase;

import java.io.File;
import java.io.IOException;

public class MainReader {
    public static void main(String[] args) throws IOException {
        String inFile = "hello.par";
        Configuration conf = new Configuration();

        File absoluteFile = new File(inFile).getAbsoluteFile();
        Path path = new Path(absoluteFile.toURI());
        FileSystem fileSystem = path.getFileSystem(conf);

        ThriftParquetReader.Builder<Msg> msgBuilder = ThriftParquetReader.build(path);
        ParquetReader<Msg> msgParquetReader = msgBuilder.withConf(conf).build();

        Msg readMsg = msgParquetReader.read();

        while (readMsg != null){
            System.out.println(readMsg.getName() + "----" +readMsg.getAge());
            readMsg = msgParquetReader.read();
        }
    }
}
