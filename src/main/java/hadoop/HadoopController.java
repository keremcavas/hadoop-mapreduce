package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import utility.Time;

import java.io.File;
import java.io.IOException;

public class HadoopController {

    private final Configuration configuration;
    private static JobListener jobListener;

    public static final int FILE_ADDED_SUCCESSFULLY = 0;
    public static final int FILE_ALREADY_EXISTS = 1;

    private static final String CORE_SITE_PATH = "/usr/local/hadoop/etc/hadoop/core-site.xml";
    private static final String HDFS_SITE_PATH = "/usr/local/hadoop/etc/hadoop/hdfs-site.xml";

    public static final String HDFS_FILE_DIRECTORY = "/usr/local/hadoop/hdfs-files";

    public HadoopController() {
        configuration = new Configuration();
        configuration.addResource(new Path(CORE_SITE_PATH));
        configuration.addResource(new Path(HDFS_SITE_PATH));
    }

    public interface JobListener {
        void addMessage(String s);
        void addMessage(long timestamp, String s);
        void refreshLastLine(String message);
        void clear();
    }

    public static void attachJobListener(JobListener listener) {
        jobListener = listener;
    }

    public int addFile(String filePath) throws IOException {

        FileSystem hdfs = FileSystem.get(configuration);

        File source = new File(filePath);

        FileUtil.copy(
                source,
                hdfs,
                new Path(HDFS_FILE_DIRECTORY + "/mapreduce-input"),
                false,
                configuration
        );

        return FILE_ADDED_SUCCESSFULLY;
    }

    public boolean deleteFile(String filePath) throws IOException {
        FileSystem hdfs = FileSystem.get(configuration);
        return hdfs.delete(new Path(filePath), true);
    }

    public void mapreduce() throws IOException, ClassNotFoundException, InterruptedException {

        jobListener.clear();

        long startTime, endTime;
        startTime = System.currentTimeMillis();

        pushMessage(startTime, "Mapreduce started");

        Job job = Job.getInstance(configuration, "mapreduce");

        job.setMapperClass(MapReducer.MapCount.class);
        job.setCombinerClass(MapReducer.ReduceCount.class);
        job.setReducerClass(MapReducer.ReduceCount.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(HDFS_FILE_DIRECTORY + "/mapreduce-input"));
        FileOutputFormat.setOutputPath(job, new Path(HDFS_FILE_DIRECTORY + "/mapreduce-output"));

        job.waitForCompletion(true);

        JobMonitor jobMonitor = new JobMonitor(job, jobListener);
        jobMonitor.monitor();

        endTime = System.currentTimeMillis();

        pushMessage(endTime, "Mapreduce finish");

        pushMessage("[INFO] total time in millis = " + (endTime - startTime));
    }

    public void max() throws IOException, ClassNotFoundException, InterruptedException {

        jobListener.clear();

        long startTime, endTime;
        startTime = System.currentTimeMillis();

        pushMessage(startTime, "Mapreduce started");

        Job job = Job.getInstance(configuration, "max");

        job.setMapperClass(MapReducer.MapMax.class);
        job.setReducerClass(MapReducer.ReduceMax.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(HDFS_FILE_DIRECTORY + "/mapreduce-output"));
        FileOutputFormat.setOutputPath(job, new Path(HDFS_FILE_DIRECTORY + "/max-output"));

        job.waitForCompletion(true);

        JobMonitor jobMonitor = new JobMonitor(job, jobListener);
        jobMonitor.monitor();

        endTime = System.currentTimeMillis();

        pushMessage(endTime, "Mapreduce finish");

        pushMessage("Total time in millis => " + (endTime - startTime));
    }

    public void average() throws IOException, ClassNotFoundException, InterruptedException {

        jobListener.clear();

        long startTime, endTime;
        startTime = System.currentTimeMillis();

        pushMessage(startTime, "Mapreduce started");

        Job job = Job.getInstance(configuration, "average");

        job.setMapperClass(MapReducer.MapAverage.class);
        job.setReducerClass(MapReducer.ReduceAverage.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(HDFS_FILE_DIRECTORY + "/mapreduce-output"));
        FileOutputFormat.setOutputPath(job, new Path(HDFS_FILE_DIRECTORY + "/average-output"));

        job.waitForCompletion(true);

        JobMonitor jobMonitor = new JobMonitor(job, jobListener);
        jobMonitor.monitor();

        endTime = System.currentTimeMillis();

        pushMessage(endTime, "Mapreduce finish");

        pushMessage("Total time in millis => " + (endTime - startTime));
    }

    public void standardDeviation() throws IOException, ClassNotFoundException, InterruptedException {

        jobListener.clear();

        long startTime, endTime;
        startTime = System.currentTimeMillis();

        pushMessage(startTime, "Mapreduce started");

        Job job = Job.getInstance(configuration, "stdev");

        job.setMapperClass(MapReducer.MapAverage.class);
        job.setReducerClass(MapReducer.ReduceAverage.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CustomWritables.StdevWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(HDFS_FILE_DIRECTORY + "/mapreduce-output"));
        FileOutputFormat.setOutputPath(job, new Path(HDFS_FILE_DIRECTORY + "/stdev-output"));

        job.waitForCompletion(true);

        JobMonitor jobMonitor = new JobMonitor(job, jobListener);
        jobMonitor.monitor();

        endTime = System.currentTimeMillis();

        pushMessage(endTime, "Mapreduce finish");

        pushMessage("Total time in millis => " + (endTime - startTime));
    }

    public void median() throws InterruptedException, IOException, ClassNotFoundException {

        jobListener.clear();

        long startTime, endTime;
        startTime = System.currentTimeMillis();

        pushMessage(startTime, "Mapreduce started");

        Job job = Job.getInstance(configuration, "median");

        job.setMapperClass(MapReducer.MapAverage.class);
        job.setReducerClass(MapReducer.ReduceAverage.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CustomWritables.MedianWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(HDFS_FILE_DIRECTORY + "/mapreduce-output"));
        FileOutputFormat.setOutputPath(job, new Path(HDFS_FILE_DIRECTORY + "/median-output"));

        job.waitForCompletion(true);

        JobMonitor jobMonitor = new JobMonitor(job, jobListener);
        jobMonitor.monitor();

        endTime = System.currentTimeMillis();

        pushMessage(endTime, "Mapreduce finish");

        pushMessage("Total time in millis => " + (endTime - startTime));
    }

    public void sum() throws InterruptedException, IOException, ClassNotFoundException {

        long startTime, endTime;
        startTime = System.currentTimeMillis();

        pushMessage(startTime, "Mapreduce started");

        Job job = Job.getInstance(configuration, "sum");

        job.setMapperClass(MapReducer.MapAverage.class);
        job.setReducerClass(MapReducer.ReduceAverage.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(HDFS_FILE_DIRECTORY + "/mapreduce-output"));
        FileOutputFormat.setOutputPath(job, new Path(HDFS_FILE_DIRECTORY + "/sum-output"));

        job.waitForCompletion(true);

        JobMonitor jobMonitor = new JobMonitor(job, jobListener);
        jobMonitor.monitor();

        endTime = System.currentTimeMillis();

        pushMessage(endTime, "Mapreduce finish");

        pushMessage("Total time in millis => " + (endTime - startTime));
    }

    private void pushMessage(String message) {
        jobListener.addMessage(message);
        System.out.println("[MESSAGE] " + message);
    }

    private void pushMessage(long timestamp, String message) {
        jobListener.addMessage(timestamp, message);
        System.out.println("[MESSAGE] " + Time.dateWithMilliseconds(timestamp) + ": " + message);
    }

}
