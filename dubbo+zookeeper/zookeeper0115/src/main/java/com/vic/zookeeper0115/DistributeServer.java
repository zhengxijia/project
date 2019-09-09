package com.vic.zookeeper0115;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class DistributeServer {
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		DistributeServer server = new DistributeServer();
		// 1获取zk连接
		server.getConnect();
		// 2 利用zk连接注册服务器信息
		server.registServer(args[0]);
		// 3 启动业务功能
		server.business(args[0]);
	}

	private void business(String hostname) throws InterruptedException {
		System.out.println(hostname+" is working ...");		
		Thread.sleep(Long.MAX_VALUE);
	}

	private void registServer(String hostname) throws KeeperException, InterruptedException {
		String create = zkServer.create("/servers/server", hostname.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		
		System.out.println(hostname +" is online "+ create);
		
	}

	private String connectString="hadoop102,hadoop103,hadoop104";
	private int sessionTimeout=2000;
	private ZooKeeper zkServer = null;

	private void getConnect() throws IOException {
		zkServer = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			
			@Override
			public void process(WatchedEvent event) {
				
			}
		});
	}
}
