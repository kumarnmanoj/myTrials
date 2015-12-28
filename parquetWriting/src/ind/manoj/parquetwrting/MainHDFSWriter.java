package ind.manoj.parquetwrting;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.thrift.ThriftParquetWriter;

import java.io.File;
import java.io.IOException;

public class MainHDFSWriter {
    public static void main(String[] args) throws IOException {
        String outFilePath = "hdfs://localhost:9000/parquet/hello.par";
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");

//        File outFile = new File(outFilePath).getAbsoluteFile();
        Path path = new Path(outFilePath);
        FileSystem fileSystem = path.getFileSystem(conf);
        fileSystem.delete(path,true);
        ThriftParquetWriter thriftParquetWriter =
                new ThriftParquetWriter(
                        path,
                        Msg.class,
                        CompressionCodecName.UNCOMPRESSED,
                        ParquetWriter.DEFAULT_BLOCK_SIZE,
                        ParquetWriter.DEFAULT_PAGE_SIZE,
                        ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                        ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                        conf
                );

        for (int i = 0; i < 10; i++) {
            Msg msg = new Msg();
            msg.setName("Name_"+i);
            msg.setAge((i+1) * (10 + i));
            thriftParquetWriter.write(msg);
        }

        thriftParquetWriter.close();
    }
}
