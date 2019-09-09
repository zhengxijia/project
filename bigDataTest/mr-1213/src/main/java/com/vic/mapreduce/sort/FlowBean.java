package com.vic.mapreduce.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FlowBean implements WritableComparable<FlowBean> {

	private long upFlow;
	private long downFlow;
	private long sumFlow;

	// 无参构造方法,反射时用到
	public FlowBean() {
		super();
	}

	public FlowBean(long upFlow, long downFlow) {
		super();
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		sumFlow = upFlow + downFlow;
	}

	// 序列化方法
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);
	}

	// 反序列化方法
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		upFlow = in.readLong();
		downFlow = in.readLong();
		sumFlow = in.readLong();
	}

	// 比较方法
	@Override
	public int compareTo(FlowBean bean) {

		int result;
			
		// 按照总流量大小，倒序排列
		if (sumFlow > bean.getSumFlow()) {
			result = -1;
		}else if (sumFlow < bean.getSumFlow()) {
			result = 1;
		}else {
			result = 0;
		}

		return result;
	}

	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getDownFlow() {
		return downFlow;
	}

	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}

	public long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}

	@Override
	public String toString() {
		return upFlow + "\t" + downFlow + "\t" + sumFlow ;
	}
	
	

}
