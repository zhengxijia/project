package com.vic.mapreduce.table;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TableBean implements Writable{

	private String id; // 订单id
	private String pid;      // 产品id
	private int amount;       // 产品数量
	private String pname;     // 产品名称
	private String flag;      // 表的标记
	
	//无参构造函数,提供反射用
	public TableBean() {
		super();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		 out.writeUTF(id);
		 out.writeUTF(pid);
		 out.writeInt(amount);
		 out.writeUTF(pname);
		 out.writeUTF(flag);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		id = in.readUTF();
		pid = in.readUTF();
		amount = in.readInt();
		pname = in.readUTF();
		flag = in.readUTF();
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getPid() {
		return pid;
	}

	public void setPid(String pid) {
		this.pid = pid;
	}

	public int getAmount() {
		return amount;
	}

	public void setAmount(int amount) {
		this.amount = amount;
	}

	public String getPname() {
		return pname;
	}

	public void setPname(String pname) {
		this.pname = pname;
	}

	public String getFlag() {
		return flag;
	}

	public void setFlag(String flag) {
		this.flag = flag;
	}

	@Override
	public String toString() {
		return id + "\t" + amount + "\t" + pname;
	}

	
}
