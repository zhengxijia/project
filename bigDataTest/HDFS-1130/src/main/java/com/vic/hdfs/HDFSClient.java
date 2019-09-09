package com.vic.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

public class HDFSClient {
	@Test
	public void testMkdirs() throws IOException, InterruptedException, URISyntaxException {

		// 1 获取文件系统
		Configuration configuration = new Configuration();
		// 配置在集群上运行
		// configuration.set("fs.defaultFS", "hdfs://hadoop102:9000");
		// FileSystem fs = FileSystem.get(configuration);

		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "vic");

		// 2 创建目录
		fs.mkdirs(new Path("/1108/daxian/banzhang"));

		// 3 关闭资源
		fs.close();
	}

	// 文件上传
	@Test
	public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {

		// 1 获取文件系统
		Configuration configuration = new Configuration();
		configuration.set("dfs.replication", "2");
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "vic");

		// 2 上传文件
		fs.copyFromLocalFile(new Path("e:/banzhang.txt"), new Path("/banhua.txt"));

		// 3 关闭资源
		fs.close();

		System.out.println("over");
	}

	// 文件下载
	@Test
	public void testCopyToLocalFile() throws IOException, InterruptedException, URISyntaxException {
		// 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "vic");

		// fs.copyToLocalFile(new Path("/panjinglian.txt"), new Path("e:/"));
		fs.copyToLocalFile(false, new Path("/panjinglian.txt"), new Path("e:/xiaoming.txt"), true);
		fs.close();
		System.out.println("over");
	}

	// 文件删除
	@Test
	public void testDelete() throws IOException, InterruptedException, URISyntaxException {

		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "vic");
		fs.delete(new Path("/1108"), true);
		fs.close();
		System.out.println("over");
	}

	@Test
	public void testRename() throws IOException, InterruptedException, URISyntaxException {
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "vic");
		fs.rename(new Path("/banzhang.txt"), new Path("/xiaozhang.txt"));
		fs.close();
		System.out.println("over");
	}

	@Test
	public void testListFiles() throws IOException, InterruptedException, URISyntaxException {
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "vic");
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		while (listFiles.hasNext()) {
			LocatedFileStatus status = listFiles.next();
			System.out.println(status.getPath().getName());
			System.out.println(status.getLen());
			System.out.println(status.getPermission());
			System.out.println(status.getGroup());
			BlockLocation[] blockLocations = status.getBlockLocations();
			for (BlockLocation blockLocation : blockLocations) {
				String[] hosts = blockLocation.getHosts();
				for (String host : hosts) {
					System.out.println(host);

				}
			}
			System.out.println("===========================");
		}
		fs.close();
		System.out.println("over");
	}

	@Test
	public void testListStatus() throws IOException, InterruptedException, URISyntaxException {

		// 1 获取文件配置信息
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

		// 2 判断是文件还是文件夹
		FileStatus[] listStatus = fs.listStatus(new Path("/"));

		for (FileStatus fileStatus : listStatus) {

			// 如果是文件
			if (fileStatus.isFile()) {
				System.out.println("f:" + fileStatus.getPath().getName());
			} else {
				System.out.println("d:" + fileStatus.getPath().getName());
			}
		}

		// 3 关闭资源
		fs.close();
	}
}
