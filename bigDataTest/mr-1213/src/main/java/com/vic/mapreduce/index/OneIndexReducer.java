package com.vic.mapreduce.index;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OneIndexReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	IntWritable v = new IntWritable();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int sum=0;
		for (IntWritable intWritable : values) {
			sum += 1;
		}
		v.set(sum);
		context.write(key, v);
	}
}
