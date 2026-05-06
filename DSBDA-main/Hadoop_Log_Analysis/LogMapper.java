import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text logLevel = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Example: split log line
        String[] parts = line.split(" ");

        if (parts.length > 2) {
            String level = parts[2]; // INFO / ERROR
            logLevel.set(level);
            context.write(logLevel, one);
        }
    }
}