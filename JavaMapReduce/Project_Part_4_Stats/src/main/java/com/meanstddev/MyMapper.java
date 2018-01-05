package com.meanstddev;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<Object, Text,Text, DoubleWritable> {
	
	private Text salaryType = new Text();
	private DoubleWritable salary = new DoubleWritable();
	
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		
		String fields[] = value.toString().split(",");

	
        if(!fields[2].contains("Salary Frequency")){
		salaryType.set(fields[2]);
		try {
			salary.set(Double.parseDouble(fields[1]));
			}
			catch(Exception e) {
				System.out.println("Caught exception");
			}
        }
        
        if(!fields[2].contains("Salary Frequency")){
        context.write(salaryType, salary);
        }
	}

}
