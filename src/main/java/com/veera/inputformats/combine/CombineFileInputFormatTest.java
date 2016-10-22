package com.veera.inputformats.combine;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CombineFileInputFormatTest implements Tool {
	
	
	
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
		conf.set("mapred.max.split.size", "134217728");//128 MB  		
		Job job = new Job(getConf());
		job.setJobName("DriverCombineFileInputFormat");
		job.setJarByClass(getClass());
		job.setMapperClass(CombineFileInputFormatMapper.class);
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(ExtendedCombineFileInputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1].concat(new Date().toString())));
		
		  /* Delete output filepath if already exists */
        FileSystem fs = FileSystem.get(getConf());

        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }
		return job.waitForCompletion(true)==true?0:-1;
/*
        ExtendedCombineFileInputFormat.addInputPath(conf, new Path(jobArgs[0]));  

        conf.setNumReduceTasks(0);  

        conf.setOutputFormat(TextOutputFormat.class);  
        TextOutputFormat.setOutputPath(conf, new Path(jobArgs[1]));*/  
		
       
	}

	public static void main(String[] args)throws Exception {
		ToolRunner.run(new Configuration(), new CombineFileInputFormatTest(), args);

	}

  public static class CombineFileInputFormatMapper extends Mapper<LongWritable, Text, Text, Text> {  

	Text txtKey = new Text("");  
	Text txtValue = new Text("");  
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

    if (value.toString().length() > 0) {  
             String[] arrEmpAttributes = value.toString().split("\\t");  
             txtKey.set(arrEmpAttributes[0].toString());  
             txtValue.set(arrEmpAttributes[2].toString() + "\t"  
                               + arrEmpAttributes[3].toString());  

             context.write(txtKey, txtValue);  
    }  

}  

}  
	
}  
