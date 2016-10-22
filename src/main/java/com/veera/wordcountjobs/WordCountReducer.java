/**
 * 
 */
package com.veera.wordcountjobs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author veeraravisingiri
 *
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, LongWritable> {
    // ex: HDFS [1,1,1]
    @Override
    protected void reduce(Text key, Iterable<IntWritable> value, Reducer<Text, IntWritable, Text, LongWritable>.Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        LongWritable count = new LongWritable();

        while (value.iterator().hasNext()) {
            sum = sum + value.iterator().next().get();
        }
        count.set(sum);
        context.write(key, new LongWritable(sum));
        // HDFS, 4
    }
}
