package com.vic.mapreduce.sort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowCountSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

	FlowBean k = new FlowBean();
	Text v = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		// 1.获取一行
		String line = value.toString();
		// 2.切割
		String[] words = line.split("\t");
		// 3.封装对象
		String phoneNum = words[0];
		
		long upFlow =Long.parseLong(words[1]);
		long downFlow =Long.parseLong(words[2]);
		long sumFlow =Long.parseLong(words[3]);
		
		k.setUpFlow(upFlow);
		k.setDownFlow(downFlow);
		k.setSumFlow(sumFlow);
		
		v.set(phoneNum);
		
		//4.写出
		context.write(k, v);
	}
}
