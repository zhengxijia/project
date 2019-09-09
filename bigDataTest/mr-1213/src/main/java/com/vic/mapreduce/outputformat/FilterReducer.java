package com.vic.mapreduce.outputformat;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FilterReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
	@Override
	protected void reduce(Text key, Iterable<NullWritable> values,Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line =key.toString();
		line = line+"\r\n";
		Text k = new Text();
		k.set(line);
		for (NullWritable value : values) {
			context.write(k, NullWritable.get());
		}
	}
}
