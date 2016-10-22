package com.veera.ReduceSideJoin;

import java.sql.Driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RSideJoinDriver implements Tool {
    Configuration conf = new Configuration();

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new RSideJoinDriver(), args);
    }

    public int run(String[] allArgs) throws Exception {
        String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();

        Job job = Job.getInstance(getConf());
        job.setJarByClass(Driver.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(ProductIdKey.class);
        job.setMapOutputValueClass(JoinGenericWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, SalesOrderDataMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ProductMapper.class);

        job.setReducerClass(JoinRecuder.class);

        job.setSortComparatorClass(JoinSortingComparator.class);
        job.setGroupingComparatorClass(JoinGroupingComparator.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(allArgs[2]));
        boolean status = job.waitForCompletion(true);
        if (status) {
            return 0;
        }
        else {
            return 1;
        }
    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }
}
