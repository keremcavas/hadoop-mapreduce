package hadoop;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;

public class HdfsReader {

    public static String read(int firstNLine, Path file, FileSystem hdfs) throws IOException {

        StringBuilder output = new StringBuilder(System.lineSeparator());
        int readLine = 1;

        if (!hdfs.exists(file)) {
            return output.toString();
        }

        FSDataInputStream inputStream = hdfs.open(file);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

        String line;
        line = bufferedReader.readLine();
        output.append(line);
        while (line != null && readLine < firstNLine) {
            line = bufferedReader.readLine();
            output.append(System.lineSeparator()).append(line);
            readLine++;
        }

        return output.toString();
    }

}
