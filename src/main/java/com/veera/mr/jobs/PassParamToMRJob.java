package com.veera.mr.jobs;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PassParamToMRJob implements Tool {

	private Configuration conf;
	
	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Configuration getConf() {
		
		return conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String customeValue = args[0].toString();
		conf.set("name", customeValue);
		System.out.println("Custom Value ***********"+conf.get("name"));
		Job job = new Job(conf);
		job.setJobName("pass param to MR");
		job.setJarByClass(this.getClass());
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(2);
		job.setPartitionerClass(MyPartitioner.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2].concat(new Date().toString())));
		return job.waitForCompletion(true)==true?0:-1;
	}

	public static void main(String[] args)throws Exception {
		ToolRunner.run(new Configuration(), new PassParamToMRJob(), args);

	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		
		Text tkey = new Text();
		Text tValue = new Text();
		
		String name = "";
		@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			 name = config.get("name");
		}
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			/*Configuration config = context.getConfiguration();
			String name = config.get("name");*/
			System.out.println("Name==="+name);
			String[] sValue = value.toString().split("\t");
			
			tkey.set(sValue[1].toString());
			tValue.set(sValue[0].toString()+","+sValue[1].toString()+","+sValue[2].toString()+","+name.toString());
			context.write(tkey, tValue);

		}
	}
	
	public class MyReducer extends Reducer<Text, Text, Text, Text>
	{
		Text tValue = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> value,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			while(value.iterator().hasNext())
			{
				tValue.set(value.iterator().next().toString());
			}
			context.write(key, tValue);
		}
	}
	
public static class MyPartitioner extends Partitioner<Text, Text>
{
	@Override
	public int getPartition(Text key, Text value, int noOfPartitions) {
		
		String[] sValue =value.toString().split(",");
		System.out.println("Key==="+key+" Partitioner Value****"+java.util.Arrays.toString(sValue));
		if(key.toString().equalsIgnoreCase(sValue[3].toString()))
		{
			return 0;
		}
		else {			
		return 1;
		}
	}
	
}
	
}
