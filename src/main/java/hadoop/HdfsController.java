package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HdfsController {

    public static final int FILE_ADDED_SUCCESSFULLY = 0;
    public static final int FILE_ALREADY_EXISTS = 1;

    private static final String CORE_SITE_PATH = "/usr/local/hadoop/etc/hadoop/core-site.xml";
    private static final String HDFS_SITE_PATH = "/usr/local/hadoop/etc/hadoop/hdfs-site.xml";

    public HdfsController() {
    }

    private FileSystem getFileSystem() throws IOException {

        Configuration configuration = new Configuration();
        configuration.addResource(new Path(CORE_SITE_PATH));
        configuration.addResource(new Path(HDFS_SITE_PATH));

        return FileSystem.get(configuration);
    }

    public int addFile(String filePath) throws IOException {

        FileSystem hdfs = getFileSystem();
        Path fileToAdd = new Path(filePath);

        FSDataOutputStream outputStream;

        if (hdfs.exists(fileToAdd)) {
            System.out.println("[INFO] File already exists in hdfs");
            return FILE_ALREADY_EXISTS;
        } else {
            outputStream = hdfs.create(fileToAdd);
            outputStream.close();
            return FILE_ADDED_SUCCESSFULLY;
        }

    }

}
