package ind.manoj.parquetwrting;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.thrift.ThriftParquetWriter;

import java.io.File;
import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        String outFilePath = "hello.par";
        Configuration conf = new Configuration();

        File outFile = new File(outFilePath).getAbsoluteFile();
        Path path = new Path(outFile.toURI());
        FileSystem fileSystem = path.getFileSystem(conf);
        fileSystem.delete(path,true);
        ThriftParquetWriter thriftParquetWriter = new ThriftParquetWriter(path, Msg.class, CompressionCodecName.UNCOMPRESSED);

        for (int i = 0; i < 10; i++) {
            Msg msg = new Msg();
            msg.setName("Name_"+i);
            msg.setAge((i+1) * (10 + i));
            thriftParquetWriter.write(msg);
        }

        thriftParquetWriter.close();
    }
}
