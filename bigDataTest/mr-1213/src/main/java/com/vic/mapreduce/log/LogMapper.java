package com.vic.mapreduce.log;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		// 1 获取1行数据
		String line = value.toString();
		// 2 解析日志
		boolean result = parseLog(line,context);
		// 3 日志不合法退出		
		if(!result){
			return;
		}
		// 4 写出数据
		context.write(value, NullWritable.get());
	}

	private boolean parseLog(String line, Context context) {
		// TODO Auto-generated method stub
		String[] words = line.split(" ");
		if(words.length>11){
			
			context.getCounter("map", "true").increment(1);
			return true;
		}else{
			context.getCounter("map", "false").increment(1);
			return false;
		}	
	}
}
