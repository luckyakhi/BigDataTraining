package com.veera.inputformats;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SequenceFileInputExample implements Tool {

	private Configuration conf;

	@Override
	public int run(String[] args) throws Exception {
		Job wordCountPerFileJob = new Job(getConf());
		wordCountPerFileJob.setJobName(" Generators ");
		wordCountPerFileJob.setJarByClass(this.getClass());

		wordCountPerFileJob.setMapperClass(WordCountPerFileMapper.class);
		wordCountPerFileJob.setMapOutputKeyClass(Text.class);
		wordCountPerFileJob.setMapOutputValueClass(Text.class);

//		wordCountPerFileJob.setReducerClass(WordCountPerFileReducer.class);
//		wordCountPerFileJob.setOutputKeyClass(WordCountPerFileKey.class);
//		wordCountPerFileJob.setOutputValueClass(IntWritable.class);

		wordCountPerFileJob.setInputFormatClass(SequenceFileInputFormat.class);

		FileInputFormat.setInputPaths(wordCountPerFileJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(wordCountPerFileJob, new Path(args[1]));

		return wordCountPerFileJob.waitForCompletion(true) == true ? 0 : -1;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new SequenceFileInputExample(), args);
	}

	public static class TempCountPerFileMapper extends Mapper<Text, Text, Text, Text> {

		private final static IntWritable one = new IntWritable(1);

		@Override
		protected void map(Text key, Text value, Context context) throws java.io.IOException, InterruptedException {
			String fileName = key.toString();
			/*String content = value.toString();
			StringTokenizer strTock = new StringTokenizer(content, " ");
			while (strTock.hasMoreTokens()) {
				WordCountPerFileKey wcKey = new WordCountPerFileKey();
				wcKey.setFileName(fileName);
				wcKey.setWord(strTock.nextToken());
				context.write(wcKey, one);
			}*/
			context.write(key, value);
		};

	}
	
	public static class WordCountPerFileMapper extends Mapper<Text, Text, WordCountPerFileKey, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

		@Override
		protected void map(Text key, Text value, Context context) throws java.io.IOException, InterruptedException {
			String fileName = key.toString();
//			String content = value.toString();
			StringTokenizer strTock = new StringTokenizer(value.toString(), " ");
			while (strTock.hasMoreTokens()) {
				WordCountPerFileKey wcKey = new WordCountPerFileKey();
				wcKey.setFileName(fileName);
				wcKey.setWord(strTock.nextToken());
				context.write(wcKey, one);
			}
			
			
//			context.write(key, value);
		};

	}
	
}
