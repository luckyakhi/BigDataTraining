package com.veera.mr.jobs;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InverseMapper extends Mapper<LongWritable, Text, Text, Text> {

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
