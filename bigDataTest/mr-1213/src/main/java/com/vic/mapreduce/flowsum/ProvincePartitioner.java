package com.vic.mapreduce.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text, FlowBean>{

	@Override
	public int getPartition(Text key, FlowBean value, int numPartitions) {
		// TODO Auto-generated method stub
		String prePhoneNum = key.toString().substring(0, 3);
		int partition = 4;
		if("136".equals(prePhoneNum)){
			partition = 0;
		}else if("137".equals(prePhoneNum)){
			partition = 1;
		}else if("138".equals(prePhoneNum)){
			partition = 2;
		}else if("139".equals(prePhoneNum)){
			partition = 3;
		}
		return partition;
	}

}
