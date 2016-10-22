package com.veera.MapSideJoins;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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

public class MapSideJoin {

	public static void main(String[] args) throws IOException,
			InterruptedException {
		JobConf conf = new JobConf("Map Side Join");
		conf.setJarByClass(MapSideJoin.class);
		conf.setMapperClass(MapSideJoinMapper.class);
		conf.setInputFormat(CompositeInputFormat.class);
		String strJoinStmt = CompositeInputFormat.compose("inner",
				KeyValueTextInputFormat.class, new Path(args[0]), new Path(
						args[1]));
		conf.set("mapred.join.expr", strJoinStmt);
		conf.setNumReduceTasks(0);
		conf.setOutputFormat(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(conf, new Path(args[2]));
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);
		RunningJob job = JobClient.runJob(conf);
		while (!job.isComplete()) {
			Thread.sleep(1000);
		}
		System.exit(job.isSuccessful() ? 0 : 2);
	}
	public static class MapSideJoinMapper extends MapReduceBase implements
	Mapper<Text, TupleWritable, Text, LongWritable> {

private LongWritable value10 = new LongWritable();

@Override
public void map(Text key, TupleWritable value,
		OutputCollector<Text, LongWritable> context, Reporter reporter)
		throws IOException {
	if (value.toString().length() == 2) {
		Long value1 = Long.valueOf(value.get(0).toString());
		Long value2 = Long.valueOf(value.get(1).toString());
		value10.set(value1 + value2);
		context.collect(key, value10);
	}
}

}
}
