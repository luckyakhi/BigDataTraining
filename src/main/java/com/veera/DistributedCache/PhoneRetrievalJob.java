package com.veera.DistributedCache;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@SuppressWarnings("deprecation")
public class PhoneRetrievalJob implements Tool {

	private Configuration conf;

	@Override
	public int run(String[] args) throws Exception {
		Job phoneJob = new Job(getConf());
		phoneJob.setJobName("Phonebook lookup JOb");
		phoneJob.setJarByClass(this.getClass());

		phoneJob.setNumReduceTasks(0);

		phoneJob.setMapperClass(PhoneRetrievalMapper.class);
		phoneJob.setOutputKeyClass(Text.class);
		phoneJob.setOutputValueClass(Text.class);

		phoneJob.setInputFormatClass(KeyValueTextInputFormat.class);

		Path lookupPath = new Path(args[2]);
		URI[] uris = new URI[1];
		uris[0] = lookupPath.toUri();
 
//		DistributedCache.setCacheFiles(uris, phoneJob.getConfiguration());
		//DistributedCache.setLocalFiles(phoneJob.getConfiguration(), args[2]);
		DistributedCache.addCacheFile(uris[0], conf);
		String inputPaths = args[0];
		String[] inputPathsArray = StringUtils.split(inputPaths, ',');
		
		Path[] inputPath = new Path[inputPathsArray.length];
		int i = 0;
		for(String inputPathStr : inputPathsArray) 
		{
			inputPath[i++] = new Path(inputPathStr);
		}
		
		FileInputFormat.setInputPaths(phoneJob, inputPath);
		FileOutputFormat.setOutputPath(phoneJob, new Path(args[1]));

		return phoneJob.waitForCompletion(true) == true ? 0 : -1;
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
		
		Configuration configuration = new Configuration();
		configuration.set("hadoop.tmp.dir", "/home/user/work/tmp");
		ToolRunner.run(configuration, new PhoneRetrievalJob(), args);
	}
public static class PhoneRetrievalMapper extends Mapper<Text, Text, Text, Text> {

	private BufferedReader reader;
	private Text phone = new Text();
	private Path lookUp;

	@Override
	protected void setup(Context context) throws java.io.IOException,
			InterruptedException {
		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context
				.getConfiguration());
		lookUp = cacheFiles[0];
	};

	@Override
	protected void map(Text key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		if(key.toString().trim().isEmpty()) {
			return;
		}
		File file = new File(lookUp.toString());
		reader = new BufferedReader(new InputStreamReader(new FileInputStream(
				file)));

		String line;
		while ((line = reader.readLine()) != null) {
			if (line.contains(key.toString())) {
				String phoneNum = line.split("\t")[1];
				phone.set(phoneNum);
				context.write(key, phone);
				break;
			}
		}
		reader.close();
	};

}

}
