package com.vic.mapreduce.cache;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	Map<Object, String> pdMap = new HashMap<>();
	Text k = new Text();
	// 缓存小表
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		URI[] cacheFiles = context.getCacheFiles();
		String path = cacheFiles[0].getPath().toString();
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));

		String line;
		while (StringUtils.isNotEmpty(line = reader.readLine())) {
			String[] words = line.split("\t");
			pdMap.put(words[0], words[1]);
		}
		// 关闭资源
		IOUtils.closeStream(reader);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		String line = value.toString();
		
		String[] words = line.split("\t");

		String pid = words[1];
		
		String pname = pdMap.get(pid);
		
		line = line +"\t"+pname;
		
		k.set(line);
		
		context.write(k, NullWritable.get());
		
	}
}
