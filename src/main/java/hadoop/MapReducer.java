package hadoop;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import utility.Pair;

import java.io.IOException;
import java.util.*;

public class MapReducer {

    public static void main(String[] args) {

        double x = (double)5/3;
        System.out.println(x);

        /*
        String line = "-----     fdgdffg    dfgd    Forwarded by Sara Shackleton/HOU/ECT on 03/28/2001 05:22 PM -----";
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            System.out.println(tokenizer.nextToken());
        }
         */
    }

    private static class Checker {

        private final static String[] trashStartingWords = {"Date:", "From:", "To:", "Subject:", "Cc:", "Mime-Version:",
                "Content-Type:", "Content-Transfer-Encoding:", "Bcc:", "X-From:", "X-To:", "X-cc:", "X-bcc:", "X-Folder:",
                "X-Origin:", "X-FileName:", "-----", "cc:", "\"", "Sent:", "Flight", "Depart:", "Arrive:", "Seats:",
                "Freq.", "Meal:", "Status:", "Return-Path:", "Message-ID:", "Received:"};

        private final static String[] trashWords = {">", ">>", "\"", ",", "-----", "[IMAGE]"};

        protected static boolean lineCheck(String line) {

            StringTokenizer tokenizer = new StringTokenizer(line);
            if (tokenizer.hasMoreTokens()) {
                String first = tokenizer.nextToken();

                if (first.charAt(0) == '\"') {
                    return false;
                }

                for (String trashWord : trashStartingWords) {
                    if (first.equals(trashWord)) {
                        return false;
                    }
                }

            } else {
                return false;
            }

            return true;
        }

        protected static boolean wordCheck(String word) {
            for (String trashWord : trashWords) {
                if (word.equals(trashWord)) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Uses csv file as an input
     */
    static class MapCount extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            if (Checker.lineCheck(line)) {
                StringTokenizer tokenizer = new StringTokenizer(line);
                while (tokenizer.hasMoreTokens()) {
                    String word = tokenizer.nextToken();
                    if (Checker.wordCheck(word)) {
                        value.set(word);
                        context.write(value, new IntWritable(1));
                    }
                }
            }
        }
    }

    static class ReduceCount extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    ////////////////////////////////////    MAX     ///////////////////////////////////////////////////////////////////
    /**
     * uses output of ReduceCount as an input
     */
    static class MapMax extends Mapper<LongWritable, Text, Text, IntWritable> {

        Pair<String, Integer> currentMax;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            currentMax = new Pair<>("temp", -1);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String word;
            String count;

            // lines has ([word], [count]) format
            StringTokenizer tokenizer = new StringTokenizer(line);
            word = tokenizer.nextToken();
            count = tokenizer.nextToken();
            int x = Integer.parseInt(count);

            if (x > currentMax.getValue()) {
                currentMax = new Pair<>(word, x); // save max occurred word, but do not write to file
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            // write final max to the file
            context.write(new Text(currentMax.getKey()), new IntWritable(currentMax.getValue()));
        }
    }

    static class ReduceMax extends Reducer<Text, IntWritable, Text, IntWritable> {

        Pair<String, Integer> currentMax;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            currentMax = new Pair<>("temp", -1);
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            String word = key.toString();
            int count = 0;

            for (IntWritable value : values) {
                count = value.get(); // 'values' iterable has only one element (word occurrence)
            }

            if (count > currentMax.getValue()) {
                currentMax = new Pair<>(word, count); // save max occurred word, but do not write to file
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            // write final max to the file
            context.write(new Text(currentMax.getKey()), new IntWritable(currentMax.getValue()));
        }
    }

    /////////////////////////////////     AVERAGE      ///////////////////////////////////////////////////////////////
    /**
     * uses output of ReduceCount as an input
     */
    static class MapAverage extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        double count;
        double sum;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            count = 0;
            sum = 0;
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);

            tokenizer.nextToken(); // first token is key (word), and do not needed for calculating average
            sum += Integer.parseInt(tokenizer.nextToken());
            count++;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text(String.valueOf(count)), new DoubleWritable(sum));
        }
    }

    static class ReduceAverage extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        double count;
        double sum;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            count = 0;
            sum = 0;
        }

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

            // file has ([count], [sum]) format
            count += Double.parseDouble(key.toString());

            for (DoubleWritable value : values) {
                sum += value.get(); // 'values' iterable has only one element (sum)
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("average"), new DoubleWritable(sum / count));
        }
    }

    /////////////////////////////////     STANDARD DEVIATION     /////////////////////////////////////////////////////
    /**
     * uses output of ReduceCount as an input
     */
    static class MapStdev extends Mapper<LongWritable, Text, Text, CustomWritables.StdevWritable> {

        private double size;
        private double mean;
        private double m2; // total sum of squares of differences

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            size = 0;
            mean = 0;
            m2 = 0;
        }

        /**
         * https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm
         * the mean and the sum of squares of differences has found by single loop
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String count;

            StringTokenizer tokenizer = new StringTokenizer(line);
            tokenizer.nextToken(); // first element of line is key (word) and it does not needed for counting stdev
            count = tokenizer.nextToken();
            int x = Integer.parseInt(count);

            size++;
            double oldDelta = x - mean;
            mean += (double)x/size;
            double newDelta = x - mean;
            m2 += oldDelta * newDelta;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            // map outputs won't combine in same list before reduce. every line should be kept separate
            // so we have to set unique key for each output line
            int uniqueId = context.getTaskAttemptID().getTaskID().getId();

            CustomWritables.StdevWritable values = new CustomWritables.StdevWritable(size, mean, m2);
            context.write(new Text(String.valueOf(uniqueId)), values);
        }
    }

    static class ReduceStdev extends Reducer<Text, CustomWritables.StdevWritable, Text, DoubleWritable> {

        double size;
        double mean;
        double m2;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            size = 0;
            mean = 0;
            m2 = 0;
        }

        /**
         * parallelly computed sum of squares of differences added up by formula on the link:
         * https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
         */
        @Override
        protected void reduce(Text key, Iterable<CustomWritables.StdevWritable> values, Context context) throws IOException, InterruptedException {

            double delta;
            double newSize;
            for (CustomWritables.StdevWritable value : values) {
                newSize = value.getSize();
                delta = value.getMean() - mean;
                m2 += value.getM2() + Math.pow(delta, 2) * size * newSize / (size + newSize);
                size += newSize;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            double standardDeviation = Math.sqrt((m2 / (size - 1)));
            context.write(new Text("stdev"), new DoubleWritable(standardDeviation));
        }
    }

    ////////////////////////////////////////////////     MEDIAN    ///////////////////////////////////////////////////
    /**
     * uses output of ReduceCount as an input
     */
    static class MapMedian extends Mapper<LongWritable, Text, Text, CustomWritables.MedianWritable> {

        private HashMap<Long, Long> frequencies;
        private int pairCount;
        private int wordCount;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            frequencies = new HashMap<>();
            pairCount = 0;
            wordCount = 0;
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String count;

            StringTokenizer tokenizer = new StringTokenizer(line);
            tokenizer.nextToken(); // first element of line is key (word) and it does not needed for counting median
            count = tokenizer.nextToken();
            long x = Long.parseLong(count);

            if (frequencies.containsKey(x)) {
                long old = frequencies.get(x);
                frequencies.put(x, old + 1);
            } else {
                frequencies.put(x, 1L);
                pairCount++;
            }

            wordCount += x;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            CustomWritables.MedianWritable medianWritable = new CustomWritables.MedianWritable(pairCount, wordCount);
            for (Map.Entry<Long, Long> entry : frequencies.entrySet()) {
                medianWritable.addPair(entry.getKey(), entry.getValue());
            }
            context.write(new Text("key"), medianWritable);
        }
    }

    static class ReduceMedian extends Reducer<Text, CustomWritables.MedianWritable, Text, LongWritable> {

        private long pairCount;
        private long wordCount;
        private Map<Long, Long> frequencies;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            pairCount = 0;
            wordCount = 0;
            frequencies = new TreeMap<>(); // always sorted by key
        }

        private void putToMap(long key, long value) {
            if (frequencies.containsKey(key)) {
                long x = frequencies.get(key);
                frequencies.put(key, value + x);
            } else {
                frequencies.put(key, value);
            }
        }

        @Override
        protected void reduce(Text key, Iterable<CustomWritables.MedianWritable> values, Context context) throws IOException, InterruptedException {
            for (CustomWritables.MedianWritable value : values) {
                pairCount += value.getPairCount();
                wordCount += value.getTotalWord();
                ArrayList<Pair<Long, Long>> pairs = value.getFrequencies();
                for (Pair<Long, Long> pair : pairs) {
                    putToMap(pair.getKey(), pair.getValue());
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            final long medianIndex = wordCount / 2;
            int currentIndex = 0;
            long passedWord = 0;
            ArrayList<Map.Entry<Long, Long>> frequencyList = new ArrayList<>(frequencies.entrySet());

            while (passedWord < medianIndex) {
                passedWord += frequencyList.get(currentIndex).getKey() * frequencyList.get(currentIndex).getValue();
                if (passedWord > medianIndex) {
                    currentIndex--;
                }
                currentIndex++;
            }

            context.write(new Text("median"), new LongWritable(frequencyList.get(currentIndex).getKey()));
        }
    }

    ////////////////////////////////////    SUM     ///////////////////////////////////////////////////////////////////
    /**
     * uses output of ReduceCount as an input
     */
    static class MapSum extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String count;

            StringTokenizer tokenizer = new StringTokenizer(line);
            tokenizer.nextToken(); // first element of line is key (word) and it does not needed for counting total
            count = tokenizer.nextToken();
            long x = Integer.parseInt(count);

            value.set("total");
            context.write(value, new LongWritable(x));
        }
    }

    static class ReduceSum extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }



}
