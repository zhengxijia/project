package com.vic.zookeeper0115;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

public class TestZookeeper {
	private String connectString = "hadoop102,hadoop103,hadoop104";
	private int sessionTimeout = 2000;
	private ZooKeeper zkClient = null;

	@Before
	public void init() throws IOException {
		zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				// TODO Auto-generated method stub
				// 收到事件通知后的回调函数（用户的业务逻辑）
				try {
					List<String> children = zkClient.getChildren("/", true);
					for (String child : children) {
						System.out.println(child);
					}
				} catch (KeeperException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});

	}

	@Test
	public void create() throws KeeperException, InterruptedException {
		String create = zkClient.create("/vic", "xijiazuishuai".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

	}

	@Test
	public void getChildren() throws KeeperException, InterruptedException {
		List<String> children = zkClient.getChildren("/", true);
		for (String child : children) {
			System.out.println(child);
		}
		// 延时阻塞
		Thread.sleep(Long.MAX_VALUE);
	}
	@Test
	public void exist() throws KeeperException, InterruptedException{
		Stat stat = zkClient.exists("/vic", false);
		System.out.println(stat == null? "not exist":"exist");
	}
}
