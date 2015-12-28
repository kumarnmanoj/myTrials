package parquetwrting;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ParquetMRMapper extends Mapper<LongWritable, Msg, Void, Text> {
    @Override
    protected void map(LongWritable key, Msg value, Context context) throws IOException, InterruptedException {
        context.write(null, new Text(value.getName() + "\t" + value.getAge()));
    }
}
