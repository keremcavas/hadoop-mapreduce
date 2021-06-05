package hadoop;

import org.apache.hadoop.io.Writable;
import utility.Pair;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

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

        // default constructor for mapreduce write tasks
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

    public static class MedianWritable implements Writable {

        private long pairCount;
        private long totalWord;
        private ArrayList<Pair<Long, Long>> frequencies; // ([x], [number of words occurred x times])

        public MedianWritable(int pairCount, int totalWord) {
            frequencies = new ArrayList<>();
            this.pairCount = pairCount;
            this.totalWord = totalWord;
        }

        // default constructor for mapreduce write tasks
        public MedianWritable() {
        }

        public void setPairCount(long pairCount) {
            this.pairCount = pairCount;
        }

        public void setTotalWord(long totalWord) {
            this.totalWord = totalWord;
        }

        public void addPair(long x, long occurrences) {
            frequencies.add(new Pair<>(x, occurrences));
        }

        public long getPairCount() {
            return pairCount;
        }

        public long getTotalWord() {
            return totalWord;
        }

        public ArrayList<Pair<Long, Long>> getFrequencies() {
            return frequencies;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeLong(pairCount);
            dataOutput.writeLong(totalWord);
            for (Pair<Long, Long> frequency : frequencies) {
                dataOutput.writeLong(frequency.getKey());
                dataOutput.writeLong(frequency.getValue());
            }
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            setPairCount(dataInput.readLong());
            setTotalWord(dataInput.readLong());
            for (int i = 0; i < pairCount; i++) {
                addPair(dataInput.readLong(), dataInput.readLong());
            }
        }
    }

}
