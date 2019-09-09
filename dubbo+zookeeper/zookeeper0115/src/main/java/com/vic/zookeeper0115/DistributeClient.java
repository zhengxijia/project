package com.vic.zookeeper0115;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class DistributeClient {
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		DistributeClient client = new DistributeClient();
		// 1获取zk连接
		client.getConnect();
		// 2 获取servers的子节点信息，从中获取服务器信息列表
		client.getServerList();
		// 3 启动业务功能
		client.business();
	}

	private void business() throws InterruptedException {
		// TODO Auto-generated method stub
		System.out.println("client is working ...");
		Thread.sleep(Long.MAX_VALUE);
	}

	private void getServerList() throws KeeperException, InterruptedException {
		List<String> children = zookeeper.getChildren("/servers", true);
		ArrayList<String> servers = new ArrayList<>();
		for (String child : children) {
			byte[] data = zookeeper.getData("/servers/"+child, false, null);
			servers.add(new String(data));
		}
		 // 4打印服务器列表信息
		System.out.println(servers);
		
	}

	private String connectString="hadoop102,hadoop103,hadoop104";
	private int sessionTimeout=2000;
	private ZooKeeper zookeeper;
	private void getConnect() throws IOException {
		zookeeper = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			
			@Override
			public void process(WatchedEvent event) {
				// TODO Auto-generated method stub
				try {
					getServerList();
				} catch (KeeperException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
		
	}
}
