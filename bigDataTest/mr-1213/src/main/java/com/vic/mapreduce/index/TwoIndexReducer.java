package com.vic.mapreduce.index;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TwoIndexReducer extends Reducer<Text, Text, Text, Text>{
	Text v = new Text();
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String word="";
		
		for (Text text : values) {
			word = word + text.toString();
		}
		v.set(word);
		context.write(key, v);
	}
}
