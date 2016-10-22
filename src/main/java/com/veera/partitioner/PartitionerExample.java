package com.veera.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PartitionerExample extends Configured implements Tool
{
    // Map class

    public static class MapClass extends Mapper<LongWritable, Text, Text, Text>
    {
        public void map(LongWritable key, Text value, Context context)
        {
            try {
                String[] str = value.toString().split("\t", -3);
                String gender = str[3];
                context.write(new Text(gender), new Text(value));
            } catch (Exception e)
            {
                System.out.println(e.getMessage());
            }
        }
    }

    // Reducer class

    /*
     * public static class ReduceClass extends Reducer<Text, Text, Text, Text> {
     * 
     * public void reduce(Text key, Iterable<Text> values, Context context)
     * throws IOException, InterruptedException {
     * 
     * context.write(key, values); } }
     */

    // Partitioner class

    public static class CaderPartitioner extends
            Partitioner<Text, Text>
    {
        @Override
        public int getPartition(Text key, Text value, int numReduceTasks)
        {
            String gender = key.toString();

            if (numReduceTasks == 0)
            {
                return 0;
            }

            if (gender.equalsIgnoreCase("Male"))
            {
                return 0;
            }
            else if (gender.equalsIgnoreCase("Female"))
            {
                return 1;
            }
            else
            {
                return 2;
            }
        }
    }

    @Override
    public int run(String[] arg) throws Exception
    {
        Configuration conf = getConf();

        Job job = new Job(conf, "Partition");
        job.setJarByClass(PartitionerExample.class);

        FileInputFormat.setInputPaths(job, new Path(arg[0]));
        FileOutputFormat.setOutputPath(job, new Path(arg[1]));

        job.setMapperClass(MapClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // set partitioner statement

        job.setPartitionerClass(CaderPartitioner.class);
        // job.setReducerClass(ReduceClass.class);
        job.setNumReduceTasks(2);
        job.setInputFormatClass(TextInputFormat.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static void main(String ar[]) throws Exception
    {
        int res = ToolRunner.run(new Configuration(), new PartitionerExample(), ar);
        System.exit(0);
    }
}
