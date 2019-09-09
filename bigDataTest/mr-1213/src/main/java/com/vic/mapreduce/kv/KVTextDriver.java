package com.vic.mapreduce.kv;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.vic.mapreduce.wordcount.WordCountMapper;
import com.vic.mapreduce.wordcount.WordCountReducer;

public class KVTextDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		args = new String[] { "d:/input/inputkeyvaluetextinputformat", "d:/output" };
		Configuration conf = new Configuration();
		// 设置切割符
		conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
		// 1.获取Job对象
		Job job = Job.getInstance(conf);
		// 2.设置jar存储位置
		job.setJarByClass(KVTextDriver.class);
		// 3.关联Map和Reduce类
		job.setMapperClass(KVTextMapper.class);
		job.setReducerClass(KVTextReducer.class);
		// 4.设置Mapper阶段输出数据的key和value类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		// 5.设置最终数据输出的key和value类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// 6.设置输入路径和输出路径
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// 7.提交Job
		// job.submit();
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);

	}
}
