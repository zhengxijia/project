package com.vic.mapreduce.order;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable>{
	@Override
	protected void reduce(OrderBean key, Iterable<NullWritable> values,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	
			context.write(key, NullWritable.get());
	}
}
