package com.vic.mapreduce.compress;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class TestCompress {
	public static void main(String[] args) throws Exception {
		//压缩
		//compress("d:/input/inputcompress/hello.txt","org.apache.hadoop.io.compress.BZip2Codec");
		//compress("d:/input/inputcompress/hello.txt","org.apache.hadoop.io.compress.GzipCodec");		
		//compress("d:/input/inputcompress/hello.txt","org.apache.hadoop.io.compress.DefaultCodec");
		decompress("d:/input/inputcompress/hello.txt.deflate");
	}

	private static void decompress(String filename) throws Exception {
		// TODO Auto-generated method stub
		CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
		CompressionCodec codec = factory.getCodec(new Path(filename));
		if(codec==null){
			System.out.println("没有此压缩类型");
			return;
		}
		//1.获取输入流
		FileInputStream fis = new FileInputStream(new File(filename));
		CompressionInputStream cis = codec.createInputStream(fis);
		//2.获取输出流
		FileOutputStream fos = new FileOutputStream(new File(filename+".decoded"));
		//3.流的拷贝
		IOUtils.copyBytes(cis, fos, 1024*1024, false);
		//4.关闭资源
		IOUtils.closeStream(fos);
		IOUtils.closeStream(cis);
		IOUtils.closeStream(fis);
	}

	private static void compress(String filename, String method) throws Exception {
		// TODO Auto-generated method stub
		//1.获取输入流
		FileInputStream fis = new FileInputStream(new File(filename));
		Class<?> classCodec = Class.forName(method);
		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(classCodec, new Configuration());
		//2.获取输出流
		FileOutputStream fos = new FileOutputStream(new File(filename+codec.getDefaultExtension()));
		
		CompressionOutputStream cos = codec.createOutputStream(fos);
		//3.流的拷贝
		IOUtils.copyBytes(fis, cos, 1024*1024, false);
		//4.关闭资源
		IOUtils.closeStream(cos);
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
	}
}
