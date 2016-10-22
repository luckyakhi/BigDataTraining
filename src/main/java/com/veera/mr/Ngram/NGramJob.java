package com.veera.mr.Ngram;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.io.Text;

public class NGramJob implements Tool{

	//Configuration config = new Configuration();
	private Configuration config;
	
	@Override
	public Configuration getConf() {

		return config;
	}

	@Override
	public void setConf(Configuration config) {

		this.config = config;
	}

	@Override
	public int run(String[] args) throws Exception {
		
		Job NgramJob = new Job(getConf());
		NgramJob.setJarByClass(this.getClass());
		NgramJob.setJobName("Ngram Combinations");
		NgramJob.setMapperClass(NGramMapper.class);
		NgramJob.setReducerClass(NGramReducer.class);
		NgramJob.setMapOutputKeyClass(Text.class);
		NgramJob.setMapOutputValueClass(LongWritable.class);
		NgramJob.setOutputKeyClass(Text.class);
		NgramJob.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.setInputPaths(NgramJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(NgramJob, new Path(args[1]));

		return NgramJob.waitForCompletion(true) == true ? 0 : -1;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new NGramJob(), args);
	}

}
