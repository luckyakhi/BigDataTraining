package com.veera.mr.jobs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BiGramMapper extends
        Mapper<LongWritable, Text, BiGram, LongWritable> {

    private Text temp = new Text();
    private final static LongWritable one = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String string = value.toString();
        BiGram bigram = new BiGram();
        // StringTokenizer strTock = new StringTokenizer(string, " ");

        String tokens[] = string.split(" ");
        String first = "";
        String second = "";

        for (int i = 0; i < tokens.length; i++)
        {
            first = tokens[i];
            if (i + 1 != tokens.length)
            {
                second = tokens[i + 1];
            }
            if (!"".equals(first) && !"".equals(second) && !first.equals(second))
            {
                bigram.setWord1(first);
                bigram.setWord2(second);
                context.write(bigram, one);
            }
        }

        /*
         * String first = ""; String second = ""; if(strTock.hasMoreTokens()) {
         * first = strTock.nextToken(); } first =
         * string.substring(string.indexOf(" "));
         * System.out.println(" First Word "+first);
         * 
         * while (strTock.hasMoreTokens()) { second = strTock.nextToken();
         * bigram.setWord1(first); bigram.setWord2(second); first = second;
         * context.write(bigram, one); }
         */

    };
}
