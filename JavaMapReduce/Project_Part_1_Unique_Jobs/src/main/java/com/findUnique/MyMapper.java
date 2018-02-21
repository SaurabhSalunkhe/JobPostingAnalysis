package com.findUnique;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
	           
            String[] fields = value.toString().split(",");
          
                Text t1 = new Text(fields[0]);
                context.write(t1, NullWritable.get());
            
	
        }
		
		
}