package com.partitionpattern;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class PartitionPatternReducer extends Reducer<IntWritable, Text, Text, NullWritable> {

////    protected void reduce(IntWritable key, Iterable<Text> values,
////            Context context) throws IOException, InterruptedException {
////        for (Text value : values) {
////            context.write(value, NullWritable.get());
////        }
////    }
//	
	
//	protected void reduce(IntWritable key, Iterable<Text> values,
//            Context context) throws IOException, InterruptedException {
//		int count=0;
//        for (Text value : values) {
//        	count++;
//        }
//        context.write(count, NullWritable.get());
//    }
    
    
    
    
    
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
