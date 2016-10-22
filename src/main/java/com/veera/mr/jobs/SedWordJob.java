package com.veera.mr.jobs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class SedWordJob implements Tool{
	
	private Configuration conf = null;

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
		Job sedWordJob = new Job(getConf());
		sedWordJob.setJobName("Sed Word Job");
		sedWordJob.setJarByClass(getClass());
		sedWordJob.setMapperClass(SedWordMapper.class);
		sedWordJob.setNumReduceTasks(0);
		sedWordJob.setMapOutputKeyClass(Text.class);
		sedWordJob.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(sedWordJob,new Path(args[0]));
		FileOutputFormat.setOutputPath(sedWordJob, new Path(args[1]));

		return sedWordJob.waitForCompletion(true) == true ? 0 : -1;
	}
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new SedWordJob(), args);
		
	}
public static class SedWordMapper extends Mapper<LongWritable, Text, Text, NullWritable>{

	private Text temp = new Text();
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		if(value.toString().contains("hadoop")){
			temp.set(value.toString().replaceAll("hadoop", "Big Data"));
			context.getCounter("Sed Count", "Matched").increment(1);
			context.write(temp, NullWritable.get());
		}else
		{
			context.getCounter("Sed Count", "UnMatched").increment(1);
			context.write(value, NullWritable.get());
		}
	}
}
}
