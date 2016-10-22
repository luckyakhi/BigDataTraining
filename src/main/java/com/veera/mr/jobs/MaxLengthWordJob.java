/**
 * 
 */
package com.veera.mr.jobs;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
 * @author veeraravisingiri
 *
 */
public class MaxLengthWordJob extends Configured implements Tool {

    private Configuration conf;

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        // conf.setLong("mapred.min.spilt.size", minSplitSize);
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
     */
    @Override
    public int run(String[] args) throws Exception {

        Job maxlenthword = new Job(getConf());

        maxlenthword.setJarByClass(this.getClass());
        maxlenthword.setJobName("Maxlength Job"); // for debigging purpose

        maxlenthword.setMapperClass(MaxLengthWordMapper.class);
        maxlenthword.setReducerClass(MaxLengthReducer.class);

        maxlenthword.setMapOutputKeyClass(Text.class);
        maxlenthword.setMapOutputValueClass(LongWritable.class);

        maxlenthword.setOutputKeyClass(Text.class);
        maxlenthword.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(maxlenthword, new Path(args[0]));
        FileOutputFormat.setOutputPath(maxlenthword, new Path(args[1]));

        /* Delete output filepath if already exists */
        FileSystem fs = FileSystem.get(getConf());

        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }

        return maxlenthword.waitForCompletion(true) == true ? 0 : 1;
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new MaxLengthWordJob(), args);

        if (result == 0) {
            System.out.println("JOB SUCCESSFUL ");
        }
        else {
            System.out.println("WJOB Failure ");

        }
    }

    // word, lenghtoftheword
    public static class MaxLengthWordMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        public String temp;
        long tempLen;

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            temp = "";
            tempLen = 0;
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
                throws IOException, InterruptedException {

            String sValue = value.toString();

            StringTokenizer tokens = new StringTokenizer(sValue, " ");
            String tempToken = "";
            while (tokens.hasMoreTokens()) {

                tempToken = tokens.nextToken();

                if (tempToken.length() > temp.length()) {
                    temp = tempToken;
                    tempLen = tempToken.length();
                }
            }

        }

        @Override
        protected void cleanup(Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            context.write(new Text(temp), new LongWritable(tempLen));

        }
    }

    public static class MaxLengthReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        String temp;
        long tempLen;

        @Override
        protected void setup(Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            temp = "";
            tempLen = 0;
        }

        @Override
        protected void reduce(Text key, Iterable<LongWritable> value, Reducer<Text, LongWritable, Text, LongWritable>.Context ctx)
                throws IOException, InterruptedException {
            if (key.toString().length() > temp.length()) {
                temp = key.toString();
                tempLen = key.toString().length();

            }
        }

        @Override
        protected void cleanup(Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            context.write(new Text(temp), new LongWritable(tempLen));

        }
    }
}
