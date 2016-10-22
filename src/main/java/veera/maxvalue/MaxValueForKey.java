/**
 * 
 */
package veera.maxvalue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

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
public class MaxValueForKey implements Tool {

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
        Job MaxValueJob = new Job(getConf());

        MaxValueJob.setJarByClass(this.getClass());
        MaxValueJob.setJobName("Voting Count"); // for debigging purpose

        MaxValueJob.setMapperClass(MaxValueForKeyMapper.class);
        MaxValueJob.setReducerClass(MaxValueForKeyReducer.class);

        MaxValueJob.setMapOutputKeyClass(Text.class);
        MaxValueJob.setMapOutputValueClass(LongWritable.class);

        MaxValueJob.setOutputKeyClass(Text.class);
        MaxValueJob.setOutputValueClass(LongWritable.class);
        // MaxValueJob.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(MaxValueJob, new Path("/home/hadoop1/stringNumberpair.txt"));
        FileOutputFormat.setOutputPath(MaxValueJob, new Path("/home/hadoop1/Desktop/MaxValue"));

        /* Delete output filepath if already exists */
        FileSystem fs = FileSystem.get(getConf());

        if (fs.exists(new Path("/home/hadoop1/Desktop/MaxValue"))) {
            fs.delete(new Path("/home/hadoop1/Desktop/MaxValue"), true);
        }
        return MaxValueJob.waitForCompletion(true) == true ? 0 : 1;

    }

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new MaxValueForKey(), args);
        System.out.println("Result of wordcount job " + result);

        if (result == 0) {
            System.out.println("Result of wordcount job Success");

        }
        else {

            System.out.println("Result of wordcount job Failure");
        }
    }

    public static class MaxValueForKeyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
                throws IOException, InterruptedException {
            String sValue = value.toString();
            String[] tokens = sValue.split(",");
            if (tokens.length > 1) {
                context.write(new Text(tokens[0]), new LongWritable(Long.parseLong(tokens[1])));
            }

        }
    }

    public static class MaxValueForKeyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> value, Reducer<Text, LongWritable, Text, LongWritable>.Context ctx)
                throws IOException, InterruptedException {
            ArrayList<Long> valueList = new ArrayList<Long>();

            for (LongWritable l : value) {
                valueList.add(l.get());
            }
            Collections.sort(valueList);
            ctx.write(key, new LongWritable(valueList.get(valueList.size() - 1)));
        }
    }
}
