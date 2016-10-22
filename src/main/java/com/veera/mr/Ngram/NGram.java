/**
 * 
 */
package com.veera.mr.Ngram;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * @author KAMAKSHI THAYI
 *
 */
public class NGram {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		String line = "Beyond HDFS, YARN and MapReduce, the entire Apache Hadoop platform is now commonly considered to consist of a number of related projects as well – Apache Pig, Apache Hive, Apache HBase, Apache Spark, and others.[3]";
		line = line.replaceAll("[^a-zA-Z]+", " ");
		String tokens[] = line.split(" ");
		
		List<String> words = null;
		int n =1;
		Scanner br = new Scanner(System.in);
		System.out.println("How many number of words u want");
		
		n = br.nextInt();
		br.close();
		List<String> Ngram = null;
		
		for (int i =0;i<tokens.length - (n-1);i++)
		{
			words = new ArrayList<String>();
			Ngram = new ArrayList<String>();
			for(int j=0;j<n;j++)
			{
				if(tokens.length>j+i)
				{
					//System.out.println("J="+j+" Tokens "+tokens[j+i]);
					 
					words.add(j,tokens[j+i]);
				}
				
			}
			Ngram.addAll(words);
			words = null;
			System.out.println("N Gram "+Ngram);
			Ngram = null;
		}
		
	}

}
