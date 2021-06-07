package hadoop;

import org.apache.hadoop.io.LongWritable;
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

        private LongWritable pairCount;
        private LongWritable totalWord;
        private ArrayList<Pair<LongWritable, LongWritable>> frequencies; // ([x], [number of words occurred x times])

        public MedianWritable(LongWritable pairCount, LongWritable totalWord) {
            frequencies = new ArrayList<>();
            this.pairCount = pairCount;
            this.totalWord = totalWord;
        }

        // default constructor for mapreduce write tasks
        public MedianWritable() {
        }

        public void setPairCount(LongWritable pairCount) {
            this.pairCount = pairCount;
        }

        public void setTotalWord(LongWritable totalWord) {
            this.totalWord = totalWord;
        }

        public void addPair(long x, long occurrences) {
            frequencies.add(new Pair<>(new LongWritable(x), new LongWritable(occurrences)));
        }

        public LongWritable getPairCount() {
            return pairCount;
        }

        public LongWritable getTotalWord() {
            return totalWord;
        }

        public ArrayList<Pair<LongWritable, LongWritable>> getFrequencies() {
            return frequencies;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            pairCount.write(dataOutput);
            totalWord.write(dataOutput);
            for (Pair<LongWritable, LongWritable> frequency : frequencies) {
                frequency.getKey().write(dataOutput);
                frequency.getValue().write(dataOutput);
            }
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            pairCount.readFields(dataInput);
            totalWord.readFields(dataInput);
            LongWritable key = new LongWritable();
            LongWritable value = new LongWritable();
            for (int i = 0; i < pairCount.get(); i++) {
                key.readFields(dataInput);
                value.readFields(dataInput);
                addPair(key.get(), value.get());
            }
        }
    }

}
