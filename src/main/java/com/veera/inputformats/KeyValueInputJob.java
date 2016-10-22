package com.veera.inputformats;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class KeyValueInputJob implements Tool{
	
	private Configuration conf;
	

	@Override
	public void setConf(Configuration conf) {
		this.conf=conf;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		
		Job KeyValueInputJob = new Job(getConf());
		KeyValueInputJob.setJobName("KeyValue Input Test");
		KeyValueInputJob.setJarByClass(this.getClass());
		
		KeyValueInputJob.setMapperClass(KeyValueInputFormatMapper.class);
		KeyValueInputJob.setMapOutputKeyClass(LongWritable.class );
		KeyValueInputJob.setMapOutputValueClass(LongWritable.class);
		
		KeyValueInputJob.setInputFormatClass(KeyValueTextInputFormat.class);
		
		FileInputFormat.setInputPaths(KeyValueInputJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(KeyValueInputJob, new Path(args[1]));
		return KeyValueInputJob.waitForCompletion(true) == true ? 0 : -1;

	}
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new KeyValueInputJob(), args);
	}

public static class KeyValueInputFormatMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, LongWritable, LongWritable>.Context context)
			throws IOException, InterruptedException {
		String stringValue = value.toString();
		
		String strTock[] = stringValue.split(" ");
		
		context.write(key, new LongWritable(strTock.length));
		
		
	}

}

}
