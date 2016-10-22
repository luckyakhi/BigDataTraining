package com.veera.mr.Ngram;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NGramMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
	
	LongWritable one = new LongWritable(1);
	Text NGram = new  Text();
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		line = line.replaceAll("[^a-zA-Z]+", " ");
		String tokens[] = line.split(" ");
		
		List<String> words = null;
		int n =0;
		/*Scanner br = new Scanner(System.in);
		System.out.println("How many number of words u want");
		
		n = br.nextInt();
		br.close();*/
		n=4;
		List<String> Ngram = null;
		
		if(tokens.length >=n)
		{
		for (int i =0;i<tokens.length - (n-1);i++)
		{
			words = new ArrayList<String>();
			Ngram = new ArrayList<String>();
			for(int j=0;j<n;j++)
			{
				if(tokens.length>j+i)
				{
					words.add(j,tokens[j+i].trim());
				}
				
			}
			Ngram.addAll(words);
			
			NGram.set(Ngram.toString());
			context.write(NGram, one);
			
			//System.out.println("N Gram "+Ngram);
			Ngram = null;
		}
		
	}
		else if(line.length()>0)
		{   line = value.toString().trim();
			NGram.set(line);
			context.write(NGram, one);
		}
	}
	}


