package com.veera.inputformats;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class KeyValueTextInputFormatExample implements Tool {

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
		Job avgURLCountJob = new Job(getConf());
		avgURLCountJob.setJobName("Avg URL Word Count");
		avgURLCountJob.setJarByClass(this.getClass());
		avgURLCountJob.setMapperClass(AvgURLCountMapper.class);
		avgURLCountJob.setReducerClass(AvgURLCountReducer.class);
		avgURLCountJob.setNumReduceTasks(1);
		avgURLCountJob.setMapOutputKeyClass(Text.class);
		avgURLCountJob.setMapOutputValueClass(LongWritable.class);
		avgURLCountJob.setOutputKeyClass(Text.class);
		avgURLCountJob.setOutputValueClass(LongWritable.class);

		avgURLCountJob.setInputFormatClass(KeyValueTextInputFormat.class);
		// wordCountJob.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(avgURLCountJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(avgURLCountJob, new Path(args[1]));
		return avgURLCountJob.waitForCompletion(true) == true ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new KeyValueTextInputFormatExample(), args);
	}
	public static class AvgURLCountMapper extends Mapper<Text, Text, Text, LongWritable> {

		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			try {
				context.write(key, new LongWritable(Long.valueOf(value.toString())));
			} catch (NumberFormatException e) {
				System.out.println("Value " + value);
				e.printStackTrace();
			}
		};
	}
	public static class AvgURLCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

		@Override
		protected void reduce(Text key, Iterable<LongWritable> value, Context context) throws IOException,
				InterruptedException {
			long sum = 0;
			long count = 0;
			while (value.iterator().hasNext()) {
				sum += value.iterator().next().get();
				count++;
			}
			context.write(key, new LongWritable(sum / count));
		};
	}
}
