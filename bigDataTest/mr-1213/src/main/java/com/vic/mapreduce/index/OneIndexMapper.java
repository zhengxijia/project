package com.vic.mapreduce.index;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class OneIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	String fileName;
	Text k = new Text();
	IntWritable v = new IntWritable(1);
	//初始化
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		FileSplit split = (FileSplit)context.getInputSplit();
		Path path = split.getPath();
		fileName = path.getName();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
		String[] words = line.split(" ");
		for (String word : words) {
			k.set(word+"--"+fileName);
			context.write(k, v);
		}
	}
}
