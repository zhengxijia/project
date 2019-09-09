package com.test.gmall.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.dubbo.config.annotation.Reference;
import com.test.gmall.bean.UserAddress;
import com.test.gmall.service.OrderService;
import com.test.gmall.service.UserService;

@Service
public class OrderServiceImpl implements OrderService{
	//@Autowired
	@Reference
	UserService userService;
	public List<UserAddress> initOrder(String userId) {
		// TODO Auto-generated method stub
		System.out.println(userId);
		List<UserAddress> userAddressList = userService.getUserAddressList(userId);	
		return userAddressList;
	}
	
}
