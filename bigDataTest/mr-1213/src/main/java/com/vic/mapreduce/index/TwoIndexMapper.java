package com.vic.mapreduce.index;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TwoIndexMapper extends Mapper<LongWritable, Text, Text, Text>{
	Text k = new Text();
	Text v =new Text();
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
		String[] words = line.split("--");
		k.set(words[0]);		
		v.set(words[1].replaceAll("\t", "-->")+"\t");
		context.write(k, v);
	}
}
