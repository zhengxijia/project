package com.vic.mapreduce.table;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {
	@Override
	protected void reduce(Text key, Iterable<TableBean> values, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		ArrayList<TableBean> orderBeans = new ArrayList<>();
		TableBean pdBean = new TableBean();
		for (TableBean tableBean : values) {
			try {
				if ("order".equals(tableBean.getFlag())) {
					TableBean temp = new TableBean();
					BeanUtils.copyProperties(temp, tableBean);
					orderBeans.add(temp);

				} else {
					BeanUtils.copyProperties(pdBean, tableBean);
					
				}
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				e.printStackTrace();
			}
		}
		
		for (TableBean bean : orderBeans) {
			bean.setPname(pdBean.getPname());
			context.write(bean, NullWritable.get());
		}
	}
}
