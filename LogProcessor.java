import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogProcessor {

    // Mapper: extracts log level and emits (level, 1)
    public static class LogMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text logLevel = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // Simple parsing: assume log level exists in line
            // Example: "2024-01-01 ERROR Something failed"
            if (line.contains("ERROR")) {
                logLevel.set("ERROR");
                context.write(logLevel, one);
            } else if (line.contains("INFO")) {
                logLevel.set("INFO");
                context.write(logLevel, one);
            } else if (line.contains("WARN")) {
                logLevel.set("WARN");
                context.write(logLevel, one);
            } else if (line.contains("DEBUG")) {
                logLevel.set("DEBUG");
                context.write(logLevel, one);
            }
        }
    }

    // Reducer: sums counts for each log level
    public static class LogReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.println("Usage: LogProcessor <input_file> <output_folder>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Log Processor");

        job.setJarByClass(LogProcessor.class);

        job.setMapperClass(LogMapper.class);
        job.setCombinerClass(LogReducer.class);
        job.setReducerClass(LogReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

/*
================= SIMPLE INSTRUCTIONS =================

1. Save file as:
   LogProcessor.java

2. Create sample log file:
   echo "2024-01-01 INFO Start" > logs.txt
   echo "2024-01-01 ERROR Failure" >> logs.txt
   echo "2024-01-01 WARN Disk low" >> logs.txt
   echo "2024-01-01 INFO Running" >> logs.txt

3. Compile:
   javac -classpath $(hadoop classpath) -d . LogProcessor.java

4. Create jar:
   jar -cvf logprocessor.jar *

5. Run:
   hadoop jar logprocessor.jar LogProcessor logs.txt output

6. View output:
   cat output/part-r-00000

7. If output folder exists:
   rm -r output

=====================================================
sushant@LAPTOP-QV29UJO3:~$ cd hadoop-work
sushant@LAPTOP-QV29UJO3:~/hadoop-work$ ls
'WordCount$IntSumReducer.class'     WordCount.class   input.txt
'WordCount$TokenizerMapper.class'   WordCount.java    wc.jar
sushant@LAPTOP-QV29UJO3:~/hadoop-work$ nano logfile.txt
sushant@LAPTOP-QV29UJO3:~/hadoop-work$ ls
'WordCount$IntSumReducer.class'     WordCount.class   input.txt     wc.jar
'WordCount$TokenizerMapper.class'   WordCount.java    logfile.txt
sushant@LAPTOP-QV29UJO3:~/hadoop-work$ cd hadoop
-bash: cd: hadoop: No such file or directory
sushant@LAPTOP-QV29UJO3:~/hadoop-work$ cd
sushant@LAPTOP-QV29UJO3:~$ cd hadoop
sushant@LAPTOP-QV29UJO3:~/hadoop$ sudo service ssh start
[sudo] password for sushant:
sushant@LAPTOP-QV29UJO3:~/hadoop$ cd ~/hadoop/sbin
./start-dfs.sh
Starting namenodes on [localhost]
Starting datanodes
Starting secondary namenodes [LAPTOP-QV29UJO3]
sushant@LAPTOP-QV29UJO3:~/hadoop/sbin$ ./start-yarn.sh
Starting resourcemanager
Starting nodemanagers
sushant@LAPTOP-QV29UJO3:~/hadoop/sbin$ jps
816 DataNode
1729 Jps
1030 SecondaryNameNode
1370 NodeManager
1243 ResourceManager
sushant@LAPTOP-QV29UJO3:~/hadoop/sbin$ cd ~/hadoop-work
sushant@LAPTOP-QV29UJO3:~/hadoop-work$ nano LogProcessor.java
sushant@LAPTOP-QV29UJO3:~/hadoop-work$ javac -classpath `hadoop classpath` LogAnalysis.java
error: file not found: LogAnalysis.java
Usage: javac <options> <source files>
use --help for a list of possible options
sushant@LAPTOP-QV29UJO3:~/hadoop-work$ javac -classpath `hadoop classpath` LogProcessor.java
sushant@LAPTOP-QV29UJO3:~/hadoop-work$ jar cf log.jar LogProcessor*.class
sushant@LAPTOP-QV29UJO3:~/hadoop-work$ hdfs dfs -mkdir /loginput
hdfs dfs -put logfile.txt /loginput
mkdir: Call From LAPTOP-QV29UJO3/127.0.1.1 to localhost:9000 failed on connection exception: java.net.ConnectException: Connection refused; For more details see:  http://wiki.apache.org/hadoop/ConnectionRefused
put: Call From LAPTOP-QV29UJO3/127.0.1.1 to localhost:9000 failed on connection exception: java.net.ConnectException: Connection refused; For more details see:  http://wiki.apache.org/hadoop/ConnectionRefused
sushant@LAPTOP-QV29UJO3:~/hadoop-work$ jps
816 DataNode
2035 Jps
1030 SecondaryNameNode
1370 NodeManager
1243 ResourceManager
sushant@LAPTOP-QV29UJO3:~/hadoop-work$ sudo service ssh start
sushant@LAPTOP-QV29UJO3:~/hadoop-work$ hdfs dfs -put logfile.txt /loginput
put: Call From LAPTOP-QV29UJO3/127.0.1.1 to localhost:9000 failed on connection exception: java.net.ConnectException: Connection refused; For more details see:  http://wiki.apache.org/hadoop/ConnectionRefused
sushant@LAPTOP-QV29UJO3:~/hadoop-work$ cd ~/hadoop/sbin
sushant@LAPTOP-QV29UJO3:~/hadoop/sbin$ ./stop-dfs.sh
./stop-yarn.sh
Stopping namenodes on [localhost]
Stopping datanodes
Stopping secondary namenodes [LAPTOP-QV29UJO3]
Stopping nodemanagers
Stopping resourcemanager
sushant@LAPTOP-QV29UJO3:~/hadoop/sbin$ jps
2788 Jps
sushant@LAPTOP-QV29UJO3:~/hadoop/sbin$ cd ~/hadoop-work
sushant@LAPTOP-QV29UJO3:~/hadoop-work$ hdfs dfs -mkdir /loginput
sushant@LAPTOP-QV29UJO3:~/hadoop-work$ hdfs dfs -put logfile.txt /loginput
sushant@LAPTOP-QV29UJO3:~/hadoop-work$ hadoop jar log.jar LogProcessor /loginput /logoutput
sushant@LAPTOP-QV29UJO3:~/hadoop-work$ hdfs dfs -cat /logoutput/part-r-00000
ERROR   1
INFO    2
WARN    1

*/