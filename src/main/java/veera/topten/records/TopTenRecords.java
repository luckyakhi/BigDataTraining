/**
 * 
 */
package veera.topten.records;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author hadoop1
 *
 */
public class TopTenRecords implements Tool {

    private Configuration conf;

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.conf.Configurable#getConf()
     */
    @Override
    public Configuration getConf() {
        return conf;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.conf.Configurable#setConf(org.apache.hadoop.conf.
     * Configuration)
     */
    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
     */
    @Override
    public int run(String[] args) throws Exception {
        Job TopTenJob = new Job(getConf());

        TopTenJob.setJarByClass(this.getClass());
        TopTenJob.setJobName("Voting Count"); // for debigging purpose

        TopTenJob.setMapperClass(TopTenMapper.class);
        TopTenJob.setReducerClass(TopTenReduce.class);

        TopTenJob.setMapOutputKeyClass(Text.class);
        TopTenJob.setMapOutputValueClass(LongWritable.class);

        TopTenJob.setOutputKeyClass(Text.class);
        TopTenJob.setOutputValueClass(LongWritable.class);
        TopTenJob.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(TopTenJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(TopTenJob, new Path(args[1]));

        /* Delete output filepath if already exists */
        FileSystem fs = FileSystem.get(getConf());

        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }

        return TopTenJob.waitForCompletion(true) == true ? 0 : 1;
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new TopTenRecords(), args);

        System.out.println("Result of wordcount job " + result);

        if (result == 0) {
            System.out.println("Result of wordcount job Success");

        }
        else {

            System.out.println("Result of wordcount job Failure");
        }
    }

    public static class TopTenMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        TreeMap<String, Integer> voting = null;

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            voting = new TreeMap<String, Integer>();
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
                throws IOException, InterruptedException {
            String sValue = value.toString();
            String[] tokens = sValue.split(",");
            if (voting.containsKey(tokens[0])) {
                voting.put(tokens[0], voting.get(tokens[0]) + 1);
            }
            else {
                voting.put(tokens[0], Integer.parseInt(tokens[1]));
            }

            System.out.println("***********VEERA MAPPER*****************");

        }

        @Override
        protected void cleanup(Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, Integer> map : voting.entrySet()) {
                context.write(new Text(map.getKey()), new LongWritable(map.getValue()));
            }
        }
    }

    public static class TopTenReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
        TreeMap<String, Long> voting = null;
        Map<String, Long> sortedMap = null;

        @Override
        protected void setup(Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            voting = new TreeMap<String, Long>();

        }

        @Override
        protected void reduce(Text key, Iterable<LongWritable> value, Reducer<Text, LongWritable, Text, LongWritable>.Context ctx)
                throws IOException, InterruptedException {

            for (LongWritable noofvotes : value) {
                voting.put(key.toString(), noofvotes.get());
            }
            System.out.println("***********VEERA REDUCER*****************");

        }

        @Override
        protected void cleanup(Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            // Calling the method sortByvalues
            sortedMap = TreeMapDemo.sortByValues(voting);
            int count = 0;
            for (Entry<String, Long> map : sortedMap.entrySet()) {
                context.write(new Text(map.getKey()), new LongWritable(map.getValue()));
                count++;
                if (count == 3) {
                    break;
                }
            }

            /*
             * HashMap<String, Long> finalMap = new HashMap<String, Long>();
             * finalMap.putAll(sortedMap);
             * 
             * System.out.println("********************* MAP ***********" +
             * sortedMap);
             * 
             * for (Entry<String, Long> map : finalMap.entrySet()) { if
             * (finalMap.size() > 3) { finalMap.remove(finalMap.lastKey()); } }
             * 
             * for (Entry<String, Long> map : sortedMap.entrySet()) {
             * context.write(new Text(map.getKey()), new
             * LongWritable(map.getValue())); }
             */

            /*
             * // Calling the method sortByvalues Map sortedMap =
             * TreeMapDemo.sortByValues(voting);
             * 
             * // Get a set of the entries on the sorted map Set set =
             * sortedMap.entrySet();
             * 
             * // Get an iterator Iterator i = set.iterator();
             * 
             * // Display elements while (i.hasNext()) { Map.Entry me =
             * (Map.Entry) i.next(); System.out.print(me.getKey() + ": ");
             * System.out.println(me.getValue()); }
             */

        }
    }

    public static class TreeMapDemo {
        // Method for sorting the TreeMap based on values
        public static <K, V extends Comparable<V>> Map<K, V>
                sortByValues(final Map<K, V> map) {
            Comparator<K> valueComparator =
                    new Comparator<K>() {
                        public int compare(K k1, K k2) {
                            int compare =
                                    map.get(k1).compareTo(map.get(k2));
                            map.get(k1).compareTo(map.get(k2));
                            if (compare == 0)
                                return 1;
                            else if (compare > 0)
                                return -1;
                            else
                                return 1;
                        }
                    };

            Map<K, V> sortedByValues =
                    new TreeMap<K, V>(valueComparator);
            sortedByValues.putAll(map);
            return sortedByValues;
        }
    }
}
