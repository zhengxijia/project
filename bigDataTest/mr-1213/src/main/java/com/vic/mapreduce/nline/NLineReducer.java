package com.vic.mapreduce.nline;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NLineReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	IntWritable v =new IntWritable();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int sum=0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		v.set(sum);
		context.write(key, v);
	}
}
