package com.vic.mapreduce.flowsum;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowsumDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		args = new String[]{"d:/input/inputflow","d:/output"};
		Configuration config = new Configuration();
		// 1.获取job对象
		Job job = Job.getInstance(config);
		// 2.设置jar的路径
		job.setJarByClass(FlowsumDriver.class);
		// 3.关联mapper和reducer
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);
		// 4.设置mapper输出的key和value类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		// 5.设置最终的输出的key和value类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		job.setPartitionerClass(ProvincePartitioner.class);
		job.setNumReduceTasks(5);
		// 6.设置输入输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// 7.提交job
		// job.submit();
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
