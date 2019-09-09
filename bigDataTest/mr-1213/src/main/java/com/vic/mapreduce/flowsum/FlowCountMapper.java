package com.vic.mapreduce.flowsum;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

	Text k = new Text();
	FlowBean v = new FlowBean();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		// 7 13560436666 120.196.100.99 1116 954 200
		// id 手机号码 网络ip 上行流量 下行流量 网络状态码
		// 1.获取一行
		String line = value.toString();
		// 2.切割\t
		String[] words = line.split("\t");
		// 3.封装对象
		k.set(words[1]);
		long upFlow = Long.parseLong(words[words.length - 3]);
		long downFlow = Long.parseLong(words[words.length - 2]);
		v.setUpFlow(upFlow);
		v.setDownFlow(downFlow);
		// 4.写出
		context.write(k, v);
	}
}
