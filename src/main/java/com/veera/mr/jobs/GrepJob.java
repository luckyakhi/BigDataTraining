/**
 * 
 */
package com.veera.mr.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author veeraravisingiri
 *
 */
public class GrepJob extends Configured implements Tool {

    // Configuration conf = new Configuration();
    private Configuration conf;

    private final long minSplitSize = 67108864;

    // javac sample.java
    // java sample veera ravi
    // java wrodcount inpath outpath
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        int result = ToolRunner.run(new Configuration(), new GrepJob(), args);

        System.out.println("Result of wordcount job " + result);

        if (result == 0) {
            System.out.println("Result of wordcount job Success");

        }
        else {

            System.out.println("Result of wordcount job Failure");
        }
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        // conf.setLong("mapred.min.spilt.size", minSplitSize);
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public int run(String[] args) throws Exception {

        Job grepjob = new Job(getConf());

        grepjob.setJarByClass(this.getClass());
        grepjob.setJobName("Grep Job"); // for debigging purpose

        grepjob.setMapperClass(GrepMapper.class);

        grepjob.setMapOutputKeyClass(LongWritable.class);
        grepjob.setMapOutputValueClass(Text.class);

        // grepjob.setOutputKeyClass(Text.class);
        // grepjob.setOutputValueClass(LongWritable.class);

        grepjob.setNumReduceTasks(0);

        // grepjob.setInputFormatClass(XmLasInput.class);

        FileInputFormat.setInputPaths(grepjob, new Path(args[0]));
        FileOutputFormat.setOutputPath(grepjob, new Path(args[1]));

        return grepjob.waitForCompletion(true) == true ? 0 : 1;
    }

}
