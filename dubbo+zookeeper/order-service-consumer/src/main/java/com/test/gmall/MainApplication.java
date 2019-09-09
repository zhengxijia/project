package com.test.gmall;

import java.io.IOException;
import java.rmi.server.SocketSecurityException;

import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.test.gmall.service.OrderService;

public class MainApplication {
	public static void main(String[] args) throws IOException {
		ClassPathXmlApplicationContext ioc = new ClassPathXmlApplicationContext("consumer.xml");

		OrderService orderService = ioc.getBean(OrderService.class);
		orderService.initOrder("1");
		System.out.println("调用结束...");
		System.in.read();
	}
}
