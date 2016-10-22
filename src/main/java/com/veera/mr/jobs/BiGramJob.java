package com.veera.mr.jobs;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BiGramJob implements Tool {

    private Configuration conf;

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job wordCountJob = new Job(getConf());
        wordCountJob.setJobName("Veera Biagram Example");
        wordCountJob.setJarByClass(this.getClass());
        wordCountJob.setMapperClass(BiGramMapper.class);
        wordCountJob.setReducerClass(BiGramReducer.class);
        wordCountJob.setMapOutputKeyClass(BiGram.class);
        wordCountJob.setMapOutputValueClass(LongWritable.class);
        wordCountJob.setOutputKeyClass(BiGram.class);
        wordCountJob.setOutputValueClass(LongWritable.class);
        FileInputFormat.setInputPaths(wordCountJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(wordCountJob, new Path(args[1]));
        return wordCountJob.waitForCompletion(true) == true ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new BiGramJob(), args);
    }
    
    public static class BiGramMapper extends 	Mapper<LongWritable, Text, BiGram, LongWritable> {
    	private LongWritable one = new LongWritable(1l);

	protected void map(LongWritable key, Text value, Context context)
		throws java.io.IOException, InterruptedException {
	String temp = value.toString();
	StringTokenizer st = new StringTokenizer(temp, " ");
	if (!st.hasMoreTokens())
		return;
	String firstWord = st.nextToken();
	String secondWord = "";
	while (st.hasMoreTokens()) {
		secondWord = st.nextToken();
		BiGram bigram = new BiGram();
		bigram.setWord1(firstWord);
		bigram.setWord2(secondWord);
		context.write(bigram, one);
		firstWord = secondWord;
	}
};
}

    public static class BiGramReducer extends Reducer<BiGram, LongWritable, BiGram, LongWritable>{

    	protected void reduce(BiGram key, java.lang.Iterable<LongWritable> values, org.apache.hadoop.mapreduce.Reducer<BiGram,LongWritable,BiGram,LongWritable>.Context context) throws java.io.IOException ,InterruptedException {
    		long sum = 0;
    		while (values.iterator().hasNext()) {
    			sum += values.iterator().next().get();
    		}
    		context.write(key, new LongWritable(sum));
    	};
    }

}
