package com.veera.inputformats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MultiInputFormatDemo implements Tool {
	
	private Configuration conf;

	
	 static class StudentSchoolMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		 private Text temp = new Text();
		 private Text values = new Text();
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] line = value.toString().split("\t");
			temp.set(line[0]);
			values.set(line[1]+'\t'+"school");
			System.out.println("StudentSchoolMapper :: "+temp.toString()+":: "+values.toString());
			context.write(temp, values);
		}
	}
	
	 static class StudentDetailMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		 private Text temp = new Text();
		 private Text values = new Text();
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] line = value.toString().split("\t");
			temp.set(line[0]);
			values.set(line[1]+'\t'+"details");
			System.out.println("StudentDetailMapper :: "+temp.toString()+":: "+values.toString());
			context.write(temp, values);
		}
	}
	
	 static class StudentMarkslMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		 private Text temp = new Text();
		 private Text values = new Text();
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] line = value.toString().split("\t");
			temp.set(line[0]);
			values.set(line[1]+'\t'+line[2]+'\t'+line[3]+'\t'+"marks");
			System.out.println("StudentMarkslMapper :: "+temp.toString()+":: "+values.toString());
			context.write(temp,values );
		}
	}
	 
	 static class MyReducer extends Reducer<Text, Text, Text, Text>{
		 @Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException,
				InterruptedException {
			 Text result = new Text();
			 StringBuffer stringBuffer = new StringBuffer();
			 List<String> stdList = new ArrayList<String>();
			for(Text value: values){
//				System.out.println(value+":: "+value.toString());
				String[] word = value.toString().split("\t");
				if(word[word.length-1].equals("details"))
				{
				//stringBuffer.append(value.toString()+"\t");
					stdList.add(0, value.toString());
				}
				else if(word[word.length-1].equals("school"))
				{
					stdList.add(1, value.toString());
				}
				else if(word[word.length-1].equals("marks"))
				{
					stdList.add(2, value.toString());
				}
			}
			result.set(stdList.toString());
			context.write(key, result);
		}
	 }
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(),new MultiInputFormatDemo(), args);
	}

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

		Job MultiInputDemo = new Job(getConf());
		
		MultiInputDemo.setJobName("MultiInputFormat Demo");
		MultiInputDemo.setJarByClass(this.getClass());
		
		MultipleInputs.addInputPath(MultiInputDemo, new Path(args[0]), TextInputFormat.class, StudentSchoolMapper.class);
		MultipleInputs.addInputPath(MultiInputDemo, new Path(args[1]), TextInputFormat.class, StudentDetailMapper.class);
		MultipleInputs.addInputPath(MultiInputDemo, new Path(args[2]), TextInputFormat.class, StudentMarkslMapper.class);
		
		FileOutputFormat.setOutputPath(MultiInputDemo, new Path(args[3].concat(new Date().toString())));
		/*MultiInputDemo.setReducerClass(MyReducer.class);
//		MultiInputDemo.setNumReduceTasks(1);
		MultiInputDemo.setOutputKeyClass(Text.class);
		MultiInputDemo.setOutputValueClass(Text.class);*/
		
		MultiInputDemo.setNumReduceTasks(0);
		
		MultiInputDemo.setMapOutputValueClass(Text.class);
		
		return MultiInputDemo.waitForCompletion(true) == true ? 0 : -1;
	}

	
	
	
}
