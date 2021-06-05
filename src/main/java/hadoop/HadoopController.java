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

import javax.swing.*;
import java.io.File;
import java.io.IOException;

public class HadoopController {

    private final Configuration configuration;
    private static JobListener jobListener;
    private final FileSystem hdfs;

    public static final int FILE_ADDED_SUCCESSFULLY = 0;
    public static final int FILE_ALREADY_EXISTS = 1;

    private static final String CORE_SITE_PATH = "/usr/local/hadoop/etc/hadoop/core-site.xml";
    private static final String HDFS_SITE_PATH = "/usr/local/hadoop/etc/hadoop/hdfs-site.xml";

    public static final String HDFS_FILE_DIRECTORY = "/files";

    public HadoopController() throws IOException {
        configuration = new Configuration();
        configuration.addResource(new Path(CORE_SITE_PATH));
        configuration.addResource(new Path(HDFS_SITE_PATH));

        hdfs = FileSystem.get(configuration);
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

        jobListener.clear();
        jobListener.addMessage("File adding to hdfs");

        File source = new File(filePath);

        Path inputPath = new Path(HDFS_FILE_DIRECTORY + "/mapreduce-input");

        // add selected file to hdfs
        FileUtil.copy(
                source,
                hdfs,
                inputPath,
                false,
                configuration
        );

        jobListener.addMessage("File added to hdfs successfully");

        int firstNLine = 5;
        jobListener.addMessage("Trying to read first " + firstNLine + " line of the input file from hdfs:");
        jobListener.addMessage(HdfsReader.read(firstNLine, inputPath, hdfs));

        return FILE_ADDED_SUCCESSFULLY;
    }

    public void mapreduce() throws IOException, ClassNotFoundException, InterruptedException {

        jobListener.clear();

        long startTime, endTime;
        startTime = System.currentTimeMillis();

        pushMessage(startTime, "Mapreduce started");

        Job job = Job.getInstance(configuration, "mapreduce");

        job.setJarByClass(MapReducer.class);
        job.setMapperClass(MapReducer.MapCount.class);
        job.setCombinerClass(MapReducer.ReduceCount.class);
        job.setReducerClass(MapReducer.ReduceCount.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path outputPath = new Path(HDFS_FILE_DIRECTORY + "/mapreduce-output");

        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }

        FileInputFormat.addInputPath(job, new Path(HDFS_FILE_DIRECTORY + "/mapreduce-input"));
        FileOutputFormat.setOutputPath(job, outputPath);

        SwingWorker<Void, JobTrackerResult> swingWorker = new SwingWorker<Void, JobTrackerResult>() {
            @Override
            protected Void doInBackground() throws Exception {
                job.waitForCompletion(true);
                return null;
            }
        };
        swingWorker.execute();

        JobMonitor jobMonitor = new JobMonitor(job, jobListener);
        jobMonitor.monitor();

        endTime = System.currentTimeMillis();

        pushMessage(endTime, "Mapreduce finish");

        pushMessage("[INFO] total time in millis = " + (endTime - startTime));

        int firstNLine = 5;
        pushMessage("First " + firstNLine + " line of mapped and reduced file:");
        jobListener.addMessage(HdfsReader.read(
                firstNLine, new Path(HDFS_FILE_DIRECTORY + "/mapreduce-output/part-r-00000"), hdfs));
    }

    public void max() throws IOException, ClassNotFoundException, InterruptedException {

        jobListener.clear();

        long startTime, endTime;
        startTime = System.currentTimeMillis();

        pushMessage(startTime, "Mapreduce started");

        Job job = Job.getInstance(configuration, "max");

        job.setJarByClass(MapReducer.class);
        job.setMapperClass(MapReducer.MapMax.class);
        job.setReducerClass(MapReducer.ReduceMax.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path outputPath = new Path(HDFS_FILE_DIRECTORY + "/max-output");

        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }

        FileInputFormat.addInputPath(job, new Path(HDFS_FILE_DIRECTORY + "/mapreduce-output"));
        FileOutputFormat.setOutputPath(job, outputPath);

        SwingWorker<Void, JobTrackerResult> swingWorker = new SwingWorker<Void, JobTrackerResult>() {
            @Override
            protected Void doInBackground() throws Exception {
                job.waitForCompletion(true);
                return null;
            }
        };
        swingWorker.execute();

        JobMonitor jobMonitor = new JobMonitor(job, jobListener);
        jobMonitor.monitor();

        endTime = System.currentTimeMillis();

        pushMessage(endTime, "Mapreduce finish");

        pushMessage("Total time in millis => " + (endTime - startTime));

        jobListener.addMessage("Result of mapreduce:");
        jobListener.addMessage(HdfsReader.read(
                1, new Path(HDFS_FILE_DIRECTORY + "/max-output/part-r-00000"), hdfs));
    }

    public void average() throws IOException, ClassNotFoundException, InterruptedException {

        jobListener.clear();

        long startTime, endTime;
        startTime = System.currentTimeMillis();

        pushMessage(startTime, "Mapreduce started");

        Job job = Job.getInstance(configuration, "average");

        job.setJarByClass(MapReducer.class);
        job.setMapperClass(MapReducer.MapAverage.class);
        job.setReducerClass(MapReducer.ReduceAverage.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        Path outputPath = new Path(HDFS_FILE_DIRECTORY + "/average-output");

        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }

        FileInputFormat.addInputPath(job, new Path(HDFS_FILE_DIRECTORY + "/mapreduce-output"));
        FileOutputFormat.setOutputPath(job, outputPath);

        SwingWorker<Void, JobTrackerResult> swingWorker = new SwingWorker<Void, JobTrackerResult>() {
            @Override
            protected Void doInBackground() throws Exception {
                job.waitForCompletion(true);
                return null;
            }
        };
        swingWorker.execute();

        JobMonitor jobMonitor = new JobMonitor(job, jobListener);
        jobMonitor.monitor();

        endTime = System.currentTimeMillis();

        pushMessage(endTime, "Mapreduce finish");

        pushMessage("Total time in millis => " + (endTime - startTime));

        jobListener.addMessage("Result of mapreduce:");
        jobListener.addMessage(HdfsReader.read(
                1, new Path(HDFS_FILE_DIRECTORY + "/average-output/part-r-00000"), hdfs));
    }

    public void standardDeviation() throws IOException, ClassNotFoundException, InterruptedException {

        jobListener.clear();

        long startTime, endTime;
        startTime = System.currentTimeMillis();

        pushMessage(startTime, "Mapreduce started");

        Job job = Job.getInstance(configuration, "stdev");

        job.setJarByClass(MapReducer.class);
        job.setMapperClass(MapReducer.MapStdev.class);
        job.setReducerClass(MapReducer.ReduceStdev.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CustomWritables.StdevWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        Path outputPath = new Path(HDFS_FILE_DIRECTORY + "/stdev-output");

        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }

        FileInputFormat.addInputPath(job, new Path(HDFS_FILE_DIRECTORY + "/mapreduce-output"));
        FileOutputFormat.setOutputPath(job, outputPath);

        SwingWorker<Void, JobTrackerResult> swingWorker = new SwingWorker<Void, JobTrackerResult>() {
            @Override
            protected Void doInBackground() throws Exception {
                job.waitForCompletion(true);
                return null;
            }
        };
        swingWorker.execute();

        JobMonitor jobMonitor = new JobMonitor(job, jobListener);
        jobMonitor.monitor();

        endTime = System.currentTimeMillis();

        pushMessage(endTime, "Mapreduce finish");

        pushMessage("Total time in millis => " + (endTime - startTime));

        jobListener.addMessage("Result of mapreduce:");
        jobListener.addMessage(HdfsReader.read(
                1, new Path(HDFS_FILE_DIRECTORY + "/stdev-output/part-r-00000"), hdfs));
    }

    public void median() throws InterruptedException, IOException, ClassNotFoundException {

        jobListener.clear();

        long startTime, endTime;
        startTime = System.currentTimeMillis();

        pushMessage(startTime, "Mapreduce started");

        Job job = Job.getInstance(configuration, "median");

        job.setJarByClass(MapReducer.class);
        job.setMapperClass(MapReducer.MapMedian.class);
        job.setReducerClass(MapReducer.ReduceMedian.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CustomWritables.MedianWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path outputPath = new Path(HDFS_FILE_DIRECTORY + "/median-output");

        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }

        FileInputFormat.addInputPath(job, new Path(HDFS_FILE_DIRECTORY + "/mapreduce-output"));
        FileOutputFormat.setOutputPath(job, outputPath);

        SwingWorker<Void, JobTrackerResult> swingWorker = new SwingWorker<Void, JobTrackerResult>() {
            @Override
            protected Void doInBackground() throws Exception {
                job.waitForCompletion(true);
                return null;
            }
        };
        swingWorker.execute();

        JobMonitor jobMonitor = new JobMonitor(job, jobListener);
        jobMonitor.monitor();

        endTime = System.currentTimeMillis();

        pushMessage(endTime, "Mapreduce finish");

        pushMessage("Total time in millis => " + (endTime - startTime));

        jobListener.addMessage("Result of mapreduce:");
        jobListener.addMessage(HdfsReader.read(
                1, new Path(HDFS_FILE_DIRECTORY + "/median-output/part-r-00000"), hdfs));
    }

    public void sum() throws InterruptedException, IOException, ClassNotFoundException {

        long startTime, endTime;
        startTime = System.currentTimeMillis();

        pushMessage(startTime, "Mapreduce started");

        Job job = Job.getInstance(configuration, "sum");

        job.setJarByClass(MapReducer.class);
        job.setMapperClass(MapReducer.MapSum.class);
        job.setReducerClass(MapReducer.ReduceSum.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path outputPath = new Path(HDFS_FILE_DIRECTORY + "/sum-output");

        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }

        FileInputFormat.addInputPath(job, new Path(HDFS_FILE_DIRECTORY + "/mapreduce-output"));
        FileOutputFormat.setOutputPath(job, outputPath);

        SwingWorker<Void, JobTrackerResult> swingWorker = new SwingWorker<Void, JobTrackerResult>() {
            @Override
            protected Void doInBackground() throws Exception {
                job.waitForCompletion(true);
                return null;
            }
        };
        swingWorker.execute();

        JobMonitor jobMonitor = new JobMonitor(job, jobListener);
        jobMonitor.monitor();

        endTime = System.currentTimeMillis();

        pushMessage(endTime, "Mapreduce finish");

        pushMessage("Total time in millis => " + (endTime - startTime));

        jobListener.addMessage("Result of mapreduce:");
        jobListener.addMessage(HdfsReader.read(
                1, new Path(HDFS_FILE_DIRECTORY + "/sum-output/part-r-00000"), hdfs));
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
