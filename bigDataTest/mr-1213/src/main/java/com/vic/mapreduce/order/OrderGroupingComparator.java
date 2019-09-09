package com.vic.mapreduce.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupingComparator extends WritableComparator{
	
	protected OrderGroupingComparator() {
		super(OrderBean.class, true);
	}
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO 只要id相同,就认为是相同的key
		
		int result;
		OrderBean orderBeana = (OrderBean)a;
		OrderBean orderBeanb = (OrderBean)b;
		if (orderBeana.getOrder_id()>orderBeanb.getOrder_id()) {
			result = 1;
		}else if(orderBeana.getOrder_id()<orderBeanb.getOrder_id()){
			result = -1;
		}else{
			result = 0;
		}
		return result;
	}
}
