package parquetwrting;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.hadoop.thrift.ParquetThriftInputFormat;

public class ParquetMRDriver extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        getConf().set("parquet.thrift.read.class",Msg.class.getName());
        Job job = Job.getInstance(getConf());
        job.setJobName("Parquet MR");
        job.setJarByClass(ParquetMRDriver.class);

        job.setMapperClass(ParquetMRMapper.class);
        job.setInputFormatClass(ParquetThriftInputFormat.class);
        job.setMapOutputKeyClass(Void.class);
        job.setMapOutputValueClass(Text.class);

        Path outputFilePath = new Path("hdfs://localhost:9000/parquet-o/hello");
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/parquet/hello.par"));
        FileOutputFormat.setOutputPath(job, outputFilePath);

        FileSystem fileSystem = FileSystem.newInstance(getConf());

        if (fileSystem.exists(outputFilePath)){
            fileSystem.delete(outputFilePath, true);
        }

        job.setNumReduceTasks(0);
        return job.waitForCompletion(true) ? 0:1;
    }

    public static void main(String[] args) throws Exception{
        ParquetMRDriver parquetMRDriver = new ParquetMRDriver();
        int res = ToolRunner.run(parquetMRDriver, args);
        System.exit(res);
    }
}
