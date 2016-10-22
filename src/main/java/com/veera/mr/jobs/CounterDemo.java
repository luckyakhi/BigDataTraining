package com.veera.mr.jobs;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

class LastFMConstants {

    public static final int USER_ID = 0;
    public static final int TRACK_ID = 1;
    public static final int IS_SCROBBLED = 2;
    public static final int RADIO = 3;
    public static final int IS_SKIPPED = 4;

}

public class CounterDemo {

    private enum COUNTERS {
        INVALID_RECORD_COUNT,
        VALID_RECORDS_COUNT
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: uniquelisteners <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Unique listeners per track");
        job.setJarByClass(CounterDemo.class);
        job.setMapperClass(UniqueListenersMapper.class);
        job.setReducerClass(UniqueListenersReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        /* Delete output filepath if already exists */
        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);

        org.apache.hadoop.mapreduce.Counters counters = job.getCounters();
        System.out.println("No. of Invalid Records :"
                + counters.findCounter(COUNTERS.INVALID_RECORD_COUNT)
                        .getValue());
    }

    public static class UniqueListenersReducer extends
            Reducer<Text, Text, Text, IntWritable> {

        public void reduce(
                Text trackId,
                Iterable<Text> userIds,
                Reducer<Text, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {

            Set<String> userIdSet = new HashSet<String>();
            for (Text userId : userIds) {
                userIdSet.add(userId.toString());
            }
            IntWritable size = new IntWritable(userIdSet.size());
            context.write(trackId, size);
        }
    }

    public static class UniqueListenersMapper extends
            Mapper<Object, Text, Text, Text> {

        Text trackId = new Text();
        Text userId = new Text();

        public void map(Object key, Text value,
                Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split("[|]");
            if (parts.length > 1 && parts.length == 5) {

                trackId.set((parts[LastFMConstants.TRACK_ID]));
                userId.set((parts[LastFMConstants.USER_ID]));
                context.write(trackId, userId);
            }
            else {
                // add counter for invalid records 1+1+1
                context.getCounter(COUNTERS.INVALID_RECORD_COUNT).increment(1L);
            }

        }
    }
}