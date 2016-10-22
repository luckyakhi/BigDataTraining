package com.veera.mr.jobs;

import java.io.IOException;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InverseIndexJob implements Tool {

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

        Job inverseJob = new Job(getConf());
        inverseJob.setJarByClass(getClass());
        inverseJob.setJobName("InverseIndex Job");
        inverseJob.setMapperClass(InverseMapper.class);
        inverseJob.setReducerClass(InverseReducer.class);
        inverseJob.setMapOutputKeyClass(Text.class);
        inverseJob.setOutputValueClass(Text.class);
        inverseJob.setOutputKeyClass(Text.class);
        inverseJob.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(inverseJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(inverseJob, new Path(args[1]));

        /* Delete output filepath if already exists */
        FileSystem fs = FileSystem.get(getConf());

        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }

        return inverseJob.waitForCompletion(true) == true ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new InverseIndexJob(), args);
    }

    public static class InverseReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> value,
                Reducer<Text, Text, Text, Text>.Context context) throws IOException,
                InterruptedException {
            // String location = "";
            Text temLocation = new Text();
            Set<String> locationSet = new TreeSet<String>();
            while (value.iterator().hasNext())
            {
                // location = location +" "+ value.iterator().next().toString();
                locationSet.add(value.iterator().next().toString());
                // Collections.sort(locationSet);

            }
            temLocation.set(locationSet.toString());
            context.write(key, temLocation);
        }
    }

    public static class InverseMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            Text word = new Text();
            Text fileLocation = new Text();
            StringTokenizer values = new StringTokenizer(value.toString(), " ");
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            // fileSplit.
            String fileName = fileSplit.getPath().getName();
            fileLocation.set(fileName);

            while (values.hasMoreTokens())
            {
                word.set(values.nextToken());
                context.write(word, fileLocation);
            }

        }
    }
}
