package com.veera.mr.jobs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TriGram implements WritableComparable{
	
	String first = null;
	String second = null;
	String third = null;
			

	@Override
	public void readFields(DataInput input) throws IOException {
		
		this.first = input.readUTF();
		this.second = input.readUTF();
		this.third = input.readUTF();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(first);
		out.writeUTF(second);
		out.writeUTF(third);
	}

	@Override
	public int compareTo(Object arg0) {
		// TODO Auto-generated method stub
		return 0;
	}

	/**
	 * @return the first
	 */
	public String getFirst() {
		return first;
	}

	/**
	 * @param first the first to set
	 */
	public void setFirst(String first) {
		this.first = first;
	}

	/**
	 * @return the second
	 */
	public String getSecond() {
		return second;
	}

	/**
	 * @param second the second to set
	 */
	public void setSecond(String second) {
		this.second = second;
	}

	/**
	 * @return the third
	 */
	public String getThird() {
		return third;
	}

	/**
	 * @param third the third to set
	 */
	public void setThird(String third) {
		this.third = third;
	}

}
