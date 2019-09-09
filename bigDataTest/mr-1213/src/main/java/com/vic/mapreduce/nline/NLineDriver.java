package com.vic.mapreduce.nline;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class NLineDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// 初始化文件输入输出路径
		args = new String[] { "d:/input/inputnlinetextinputformat", "d:/output" };
		// 1.获取Job对象
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		// 2.设置Jar路径
		job.setJarByClass(NLineDriver.class);
		// 3.关联mapper和reducer
		job.setMapperClass(NLineMapper.class);
		job.setReducerClass(NLineReducer.class);
		// 4.设置mapper的输出key和value
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		// 5.设置最终输出的key和value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// 7设置每个切片InputSplit中划分三条记录
		NLineInputFormat.setNumLinesPerSplit(job, 3);
          
        // 8使用NLineInputFormat处理记录数  
		job.setInputFormatClass(NLineInputFormat.class);
		job.setNumReduceTasks(4);
		// 6.设置文件输入输出的路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// 7.提交
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
