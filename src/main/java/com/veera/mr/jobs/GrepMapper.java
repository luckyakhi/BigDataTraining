/**
 * 
 */
package com.veera.mr.jobs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author veeraravisingiri
 *
 */
public class GrepMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context) throws IOException,
            InterruptedException {
        String s = value.toString();

        if (s.contains("Hadoop")) {
            context.write(key, value);
        }

    }

}
