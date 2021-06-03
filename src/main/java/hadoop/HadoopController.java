package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class HadoopController {

    public static final int FILE_ADDED_SUCCESSFULLY = 0;
    public static final int FILE_ALREADY_EXISTS = 1;

    private static final String CORE_SITE_PATH = "/usr/local/hadoop/etc/hadoop/core-site.xml";
    private static final String HDFS_SITE_PATH = "/usr/local/hadoop/etc/hadoop/hdfs-site.xml";

    private static final String HDFS_FILE_DIRECTORY = "/usr/local/hadoop/hdfs-files";

    public HadoopController() {
    }

    private FileSystem getFileSystem() throws IOException {

        Configuration configuration = new Configuration();
        configuration.addResource(new Path(CORE_SITE_PATH));
        configuration.addResource(new Path(HDFS_SITE_PATH));

        return FileSystem.get(configuration);
    }

    public int addFile(String filePath) throws IOException {

        FileSystem hdfs = getFileSystem();


        /*
        Path fileToAdd = new Path(filePath);

        FSDataOutputStream outputStream;

        if (hdfs.exists(fileToAdd)) {
            System.out.println("[INFO] File already exists in hdfs");
            return FILE_ALREADY_EXISTS;
        } else {
            outputStream = hdfs.create(fileToAdd, new Progressable() {
                @Override
                public void progress() {

                }
            });
            outputStream.close();
            return FILE_ADDED_SUCCESSFULLY;
        }
         */

        File source = new File(filePath);

        FileUtil.copy(
                source,
                hdfs,
                new Path(HDFS_FILE_DIRECTORY + "/mapreduce-input"),
                false,
                new Configuration()
        );

        return FILE_ADDED_SUCCESSFULLY;
    }

    public void mapreduce() throws IOException, ClassNotFoundException, InterruptedException {

        long startTime, endTime;
        startTime = System.currentTimeMillis();

        Job job = Job.getInstance(new Configuration(), "mapreduce");

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

        endTime = System.currentTimeMillis();

        System.out.println("[INFO] total time in millis = " + (endTime - startTime));
    }

}
