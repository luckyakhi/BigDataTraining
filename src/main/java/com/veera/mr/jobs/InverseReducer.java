package com.veera.mr.jobs;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InverseReducer extends Reducer<Text, Text, Text, Text> {

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
