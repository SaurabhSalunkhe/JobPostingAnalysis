package com.wordcount;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Lab2Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
	       
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	               
	            String[] fields = value.toString().split(",");
	                Text t1 = new Text(fields[4]);
	                context.write(t1, one);
	            
		
	        }
}