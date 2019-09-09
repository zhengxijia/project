package com.vic.mapreduce.order;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable>{
	OrderBean k = new OrderBean();
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
		String[] words = line.split("\t");
		int order_id = Integer.parseInt(words[0]);
		double price = Double.parseDouble(words[2]);
		k.setOrder_id(order_id);
		k.setPrice(price);		
		context.write(k, NullWritable.get());
	}
}
