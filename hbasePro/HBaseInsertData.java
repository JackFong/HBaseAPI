package com.study.hbasePro;

import java.util.Scanner;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import Util.HBaseUtil;

public class HBaseInsertData {
	public static void main(String[] args) throws Exception{
		Connection conn = HBaseUtil.getHBaseConnection();
		System.out.println("成功连接至hbase");
		System.out.println("--------------插入数据---------------");
		
		Admin admin = conn.getAdmin();
		System.out.print("请输入要插入数据的表名：");
		Scanner a = new Scanner(System.in);
		String tablename = a.nextLine();
		TableName tableName = TableName.valueOf(tablename);
		
		if(admin.tableExists(tableName)) {			
			HBaseUtil.insertData(tablename);		//调用方法
		
			admin.close();
			
			System.out.println("插入数据成功!");
		}else {
			System.out.println("该表不存在！");
		}
		
		HBaseUtil.close(conn);
		System.out.println("关闭连接");
		
	}
}
