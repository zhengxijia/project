package com.vic.mapreduce.table;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean>{
	String name;
	TableBean v = new TableBean();
	Text k = new Text();
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		FileSplit inputSplit = (FileSplit) context.getInputSplit();
		name = inputSplit.getPath().getName();		
	}
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
		String[] words = line.split("\t");
		if(name.startsWith("order")){//order表
			//1001	01	1
			v.setId(words[0]);
			v.setPid(words[1]);
			v.setAmount(Integer.parseInt(words[2]));
			v.setPname("");
			v.setFlag("order");
			k.set(words[1]);
		}else{//pd表
			//01	小米
			v.setId("");
			v.setPid(words[0]);
			v.setAmount(0);
			v.setPname(words[1]);
			v.setFlag("pd");
			k.set(words[0]);
		}
		context.write(k, v);
	}
}
