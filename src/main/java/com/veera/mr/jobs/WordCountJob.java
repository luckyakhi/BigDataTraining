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
import org.apache.hadoop.io.IntWritable;
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
public class WordCountJob extends Configured implements Tool {

    // Configuration conf = new Configuration();
    private Configuration conf;

    // private final long minSplitSize = 67108864;

    // javac sample.java
    // java sample veera ravi
    // java wrodcount inpath outpath
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        int result = ToolRunner.run(new Configuration(), new WordCountJob(), args);

        System.out.println("Result of wordcount job " + result);

        if (result == 0) {
            System.out.println("Result of wordcount job Success");

        }
        else {

            System.out.println("Result of wordcount job Failure");
        }
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        // conf.setLong("mapred.min.spilt.size", minSplitSize);
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public int run(String[] args) throws Exception {

        Job wordcountjob = new Job(getConf());

        wordcountjob.setJarByClass(this.getClass());
        wordcountjob.setJobName("Word Count"); // for debigging purpose

        wordcountjob.setMapperClass(WordCountMapper.class);
        wordcountjob.setReducerClass(WordCountReducer.class);
        wordcountjob.setCombinerClass(WordCountCombiner.class);

        wordcountjob.setMapOutputKeyClass(Text.class);
        wordcountjob.setMapOutputValueClass(IntWritable.class);

        wordcountjob.setOutputKeyClass(Text.class);
        wordcountjob.setOutputValueClass(LongWritable.class);
        // wordcountjob.setNumReduceTasks(2);
        // wordcountjob.setPartitionerClass(HashPartitioner.class);

        // wordcountjob.setInputFormatClass(XmLasInput.class);

        FileInputFormat.setInputPaths(wordcountjob, new Path(args[0]));
        FileOutputFormat.setOutputPath(wordcountjob, new Path(args[1]));

        /* Delete output filepath if already exists */
        FileSystem fs = FileSystem.get(getConf());

        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }

        return wordcountjob.waitForCompletion(true) == true ? 0 : 1;
    }

    public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        // key ==> offset, value ==> line
        // Key2 ==> text, value ==> number(intWritable)
        // HDFS added the high-availability capabilities, as announced for
        // release
        // 2.0 in May 2012
        Text word = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException,
                InterruptedException {
            String line = value.toString();
            // spliting into words by single space.
            StringTokenizer tokens = new StringTokenizer(line, " ");
            int size = tokens.countTokens();
            while (tokens.hasMoreElements()) {

                // word = tokens.nextElement().toString();

                word.set(tokens.nextElement().toString());
                // ex: HDFS 1, added 1,xyz 1, abc 1

                System.out.println("************************WORD******************************" + word.toString());

                context.write(word, new IntWritable(1));

                // after sort nsd shuffle phase HDFS [1,1,1]
            }

        }

    }

    public class WordCountReducer extends Reducer<Text, IntWritable, Text, LongWritable> {
        // ex: HDFS [1,1,1]
        @Override
        protected void reduce(Text key, Iterable<IntWritable> value, Reducer<Text, IntWritable, Text, LongWritable>.Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            LongWritable count = new LongWritable();

            while (value.iterator().hasNext()) {
                sum = sum + value.iterator().next().get();
            }
            count.set(sum);
            System.out.println("**************WORD****************" + key.toString() + "######VALUE#####" + count.toString());

            context.write(key, new LongWritable(sum));
            // HDFS, 4
        }
    }

    public class WordCountCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {

        protected void reduce(Text key, Iterable<LongWritable> value, Context context) throws IOException,
                InterruptedException {
            long sum = 0;
            while (value.iterator().hasNext()) {
                sum += value.iterator().next().get();
            }
            context.write(key, new LongWritable(sum));
        };
    }

}
