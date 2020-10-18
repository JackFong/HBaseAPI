package com.study.hbasePro;

import org.apache.hadoop.hbase.client.Connection;

import Util.HBaseUtil;

public class HBaseAPI {
	public static void main(String[] args) throws Exception{
		//创建连接
		Connection conn = HBaseUtil.getHBaseConnection();
		System.out.println("创建连接成功!");
		
		//创建数据表
		HBaseUtil.createTable("student1", "Info");
		System.out.println("创建数据表成功！");
		
		//插入数据
		HBaseUtil.insertData("student1", "1001", "info", "name", "lisi");
		System.out.println("插入数据成功！");
		
		//关闭连接
		HBaseUtil.close(conn);
		System.out.println("关闭连接");
	}
}
