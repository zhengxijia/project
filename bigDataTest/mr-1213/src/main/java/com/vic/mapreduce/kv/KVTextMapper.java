package com.vic.mapreduce.kv;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KVTextMapper extends Mapper<Text, Text, Text, LongWritable>{
	//1.设置值
	LongWritable v = new LongWritable(1);
	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		context.write(key, v);
	}

}
