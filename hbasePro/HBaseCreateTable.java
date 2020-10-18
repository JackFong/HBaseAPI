package com.study.hbasePro;

import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import Util.HBaseUtil;

public class HBaseCreateTable {
	public static void main(String[] args) throws Exception{
		Connection conn = HBaseUtil.getHBaseConnection();
				
		System.out.println("成功连接至hbase");
		System.out.println("--------------创建hbase表---------------");
				
		System.out.print("请输入你要创建表的表名：");
		Scanner a = new Scanner(System.in);
		String tablename = a.nextLine();
		TableName tableName = TableName.valueOf(tablename);		
		HTableDescriptor td = new HTableDescriptor(tableName);
		
		Admin admin = conn.getAdmin();
		if(admin.tableExists(tableName)) {
			System.out.println("数据表已存在！");
		}else {				
			HBaseUtil.createTable(tablename, td);		//调用方法，创建数据表
			//最后关闭连接
			HBaseUtil.close(conn);
			System.out.println("关闭连接");
		}
	}
}
