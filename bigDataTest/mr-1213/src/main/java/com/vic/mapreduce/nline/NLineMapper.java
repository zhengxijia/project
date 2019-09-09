package com.vic.mapreduce.nline;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NLineMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	IntWritable v = new IntWritable(1);
	Text k = new Text();
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
		String[] words = line.split(" ");
		//循环写出
		for (String word : words) {
			k.set(word);
			context.write(k, v);
		}		
	}
}
