package com.vic.mapreduce.index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OneIndexDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		// 输入输出路径需要根据自己电脑上实际的输入输出路径设置
		args = new String[] { "d:/input/inputoneindex", "d:/output5" };

		Configuration config = new Configuration();
		// 1.获取job
		Job job = Job.getInstance(config);
		// 2.设置Jar路径
		job.setJarByClass(OneIndexDriver.class);
		// 3.关联mapper和reducer
		job.setMapperClass(OneIndexMapper.class);
		job.setReducerClass(OneIndexReducer.class);
		// 4.设置mapper输出格式
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		// 5.设置最终输出格式
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// 6.设置输入输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// 7.提交
		boolean result = job.waitForCompletion(true);
		System.out.println(result);
	}

}
