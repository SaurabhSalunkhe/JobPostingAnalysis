package com.meanstddev;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, DoubleWritable, Text, MedianStdDevTuple> {

	private MedianStdDevTuple result = new MedianStdDevTuple();
	private ArrayList<Double> ratings = new ArrayList<Double>();
	
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
		float sum = 0;
		float count = 0;
		
		ratings.clear();
		result.setStandardDeviation(0);
		result.setMedian(0);
		
		for(DoubleWritable val : values) {
			ratings.add(val.get());
			sum += val.get();
			++count;
		}
		
		Collections.sort(ratings);
		
		if(count % 2== 0) {
			result.setMedian((ratings.get((int) count/ 2 -1)+ ratings.get((int) count /2)) / 2.0f);
		}else {
			result.setMedian(ratings.get((int) count / 2));
		}
		
		float mean = sum/count;
		float sumOfSquares = 0.0f;
		
		for(Double f : ratings) {
			sumOfSquares += (f - mean) * (f - mean);
		}
		
		result.setStandardDeviation((float)Math.sqrt(sumOfSquares / (count -1)));
		context.write(key, result);
	}
}
