package com.vic.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class HDFSIO {
	//需求：把本地e盘上的banhua.txt文件上传到HDFS根目录
	@Test
	public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException{
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "vic");
		
		FileInputStream fis = new FileInputStream(new File("e:/xiaoming.txt"));
		
		FSDataOutputStream fos = fs.create(new Path("/xiaoming.txt"));
		
		IOUtils.copyBytes(fis, fos, configuration);
		
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);	
		fs.close();
		System.out.println("finish");
	}
	// 文件下载
	@Test
	public void getFileFromHDFS() throws IOException, InterruptedException, URISyntaxException{

		// 1 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "vic");
			
		// 2 获取输入流
		FSDataInputStream fis = fs.open(new Path("/banhua.txt"));
			
		// 3 获取输出流
		FileOutputStream fos = new FileOutputStream(new File("e:/banhua.txt"));
			
		// 4 流的对拷
		IOUtils.copyBytes(fis, fos, configuration);
			
		// 5 关闭资源
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
		fs.close();
	}
	//下载第一块
	@Test
	public void readFileSeek1() throws IOException, InterruptedException, URISyntaxException{
		//1获取对象
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "vic");
		//2获取输入流
		FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));
		//3获取输出流
		FileOutputStream fos = new FileOutputStream("e:/hadoop-2.7.2.tar.gz1");
		//4流的拷贝(只拷贝128m)
		byte[] buf = new byte[1024];
		for (int i = 0; i < 1024*128; i++) {
			fis.read(buf);
			fos.write(buf);
		}
		//5关闭资源
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
		fs.close();
	}
	
	//下载第二块
		@Test
		public void readFileSeek2() throws IOException, InterruptedException, URISyntaxException{
			//1获取对象
			Configuration configuration = new Configuration();
			FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "vic");
			//2获取输入流
			FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));			
			//设置拷贝起点
			fis.seek(1024*1024*128);
			//3获取输出流
			FileOutputStream fos = new FileOutputStream("e:/hadoop-2.7.2.tar.gz2");
			//4流的拷贝(只拷贝剩下的第二块)
			IOUtils.copyBytes(fis, fos, configuration);
			//5关闭资源
			IOUtils.closeStream(fis);
			IOUtils.closeStream(fos);
			fs.close();
		}
	
}
