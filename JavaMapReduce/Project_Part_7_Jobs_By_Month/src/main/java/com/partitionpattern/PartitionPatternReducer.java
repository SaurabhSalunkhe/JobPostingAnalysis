package com.partitionpattern;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class PartitionPatternReducer extends Reducer<IntWritable, Text, IntWritable, NullWritable> {
	
    private IntWritable total = new IntWritable();
    private IntWritable one = new IntWritable(1);
    protected void reduce(IntWritable key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
//    	total=null;
    	int count=0;
        for (Text value : values) {
        	if(value.getLength()!=0)
        count=count+1;
        }
        total.set(count);
        
        context.write(total, NullWritable.get());

        
    }
	
    
    
    
    
//    public class Lab2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
//
//        private IntWritable total = new IntWritable();
//
//       
//	public void reduce(Text key, Iterable<IntWritable> values,Context context)
//			throws IOException, InterruptedException {
//		int sum = 0;
//		for (IntWritable val : values) {
//			sum +=val.get();
//		}
//                total.set(sum);
//		context.write(key, total);
//	}
//
//}
	
	
    
    
    
    
    
//    public void reduce(Text key, Iterable<IntWritable> values,Context context)
//			throws IOException, InterruptedException {
//		int sum = 0;
//		for (IntWritable val : values) {
//			sum +=val.get();
//		}
//                total.set(sum);
//		context.write(key, total);
//	}
    
    
}
