package com.veera.MapSideJoins;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.join.CompositeInputFormat;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.util.GenericOptionsParser;

public class MapSideJoindemo {
	
	private Configuration conf;

	
	 static class StudentSchoolMapper extends MapReduceBase implements
	 Mapper<Text, TupleWritable, Text, Text> 
	{
			Text txtKey = new Text("");
			Text txtValue = new Text(""); 
		 
			public void map(Text key, TupleWritable value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException { 
			
				if (value.toString().length() > 0) {
					txtKey.set(key.toString());
					String StudentSchool[] = value.get(0).toString().split("\t");
					String StudentDetails[] = value.get(1).toString().split("\t");
					String StudentMarks[] = value.get(2).toString().split("\t");
					
					txtValue.set(StudentDetails[0].toString() + "\t"
					+ StudentSchool[0].toString() + "\t"
					+ StudentMarks[0].toString() + "\t"+StudentMarks[1].toString()+"\t"+ StudentMarks[2].toString());
					output.collect(txtKey, txtValue);
					} 
		}
	}
		

	 public static void main(String[] args) throws Exception {
		 JobConf conf = new JobConf("DriverMapSideJoinLargeDatasets");
		 conf.setJarByClass(MapSideJoindemo.class);
		 String[] jobArgs = new GenericOptionsParser(conf, args)
		 .getRemainingArgs();
		 
		 Path StudentSchool = new Path(jobArgs[0]);
		 Path StudentDetail = new Path(jobArgs[1]);
		 Path StudentMarks = new Path(jobArgs[2]);
		 Path dirOutput = new Path(jobArgs[3]);
		 
		 
		 conf.setMapperClass(StudentSchoolMapper.class);
		 conf.setInputFormat(CompositeInputFormat.class);
		 
		 String strJoinStmt = CompositeInputFormat.compose("inner",
		 KeyValueTextInputFormat.class, StudentSchool, StudentDetail,StudentMarks);
		 
		 System.out.println("strJoinStmt=="+strJoinStmt);
		 conf.set("mapred.join.expr", strJoinStmt);
		 
		 conf.setNumReduceTasks(0);
		 conf.setOutputFormat(TextOutputFormat.class);
		 TextOutputFormat.setOutputPath(conf, dirOutput);
		 conf.setOutputKeyClass(Text.class);
		 conf.setOutputValueClass(Text.class);
		 RunningJob job = JobClient.runJob(conf);
		 while (!job.isComplete()) {
		 Thread.sleep(1000);
		 }
		 System.exit(job.isSuccessful() ? 0 : 2);
		 } 

	
	
	
}
