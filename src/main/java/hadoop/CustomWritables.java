package hadoop;

import javafx.util.Pair;
import org.apache.hadoop.io.Writable;

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

        private int pairCount;
        private int totalWord;
        private ArrayList<Pair<Integer, Integer>> frequencies; // ([x], [number of words occurred x times])

        public MedianWritable(int pairCount, int totalWord) {
            frequencies = new ArrayList<>();
            this.pairCount = pairCount;
            this.totalWord = totalWord;
        }

        public void setPairCount(int pairCount) {
            this.pairCount = pairCount;
        }

        public void setTotalWord(int totalWord) {
            this.totalWord = totalWord;
        }

        public void addPair(int x, int occurrences) {
            frequencies.add(new Pair<>(x, occurrences));
        }

        public int getPairCount() {
            return pairCount;
        }

        public int getTotalWord() {
            return totalWord;
        }

        public ArrayList<Pair<Integer, Integer>> getFrequencies() {
            return frequencies;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(pairCount);
            dataOutput.writeInt(totalWord);
            for (Pair<Integer, Integer> frequency : frequencies) {
                dataOutput.writeInt(frequency.getKey());
                dataOutput.writeInt(frequency.getValue());
            }
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            setPairCount(dataInput.readInt());
            setTotalWord(dataInput.readInt());
            for (int i = 0; i < pairCount; i++) {
                addPair(dataInput.readInt(), dataInput.readInt());
            }
        }
    }

}
