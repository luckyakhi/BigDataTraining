/**
 * 
 */
package com.veera.wordcountjobs;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author veeraravisingiri
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    // key ==> offset, value ==> line
    // Key2 ==> text, value ==> number(intWritable)
    // HDFS added the high-availability capabilities, as announced for release
    // 2.0 in May 2012
    Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        String line = value.toString();
        // spliting into words by single space.
        StringTokenizer tokens = new StringTokenizer(line, " ");
        int size = tokens.countTokens();
        while (tokens.hasMoreElements()) {

            // word = tokens.nextElement().toString();

            word.set(tokens.nextElement().toString());
            // ex: HDFS 1, added 1,xyz 1, abc 1

            context.write(word, new IntWritable(1));

            // after sort nsd shuffle phase HDFS [1,1,1]
        }

    }

}
