package hadoop;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomWritables {

    public static class StdevWritable implements Writable {

        private double size;
        private double mean;
        private double m2;

        public StdevWritable(double count, double mean, double m2) {
            this.size = count;
            this.mean = mean;
            this.m2 = m2;
        }

        // do not delete, default constructor for mapreduce write tasks
        public StdevWritable() {
        }

        public void setSize(double size) {
            this.size = size;
        }

        public void setMean(double mean) {
            this.mean = mean;
        }

        public void setM2(double m2) {
            this.m2 = m2;
        }

        public double getSize() {
            return size;
        }

        public double getMean() {
            return mean;
        }

        public double getM2() {
            return m2;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeDouble(size);
            dataOutput.writeDouble(mean);
            dataOutput.writeDouble(m2);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            setSize(dataInput.readDouble());
            setMean(dataInput.readDouble());
            setM2(dataInput.readDouble());
        }
    }

}
