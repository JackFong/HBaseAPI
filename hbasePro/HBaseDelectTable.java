package com.study.hbasePro;

import java.util.Scanner;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import Util.HBaseUtil;

public class HBaseDelectTable {
	public static void main(String[] args) throws Exception{
		Connection conn = HBaseUtil.getHBaseConnection();
		System.out.println("成功连接至hbase");
		System.out.println("--------------删除表---------------");
		
		System.out.print("请输入要删除的表名：");
		Scanner a = new Scanner(System.in);
		String tablename = a.nextLine();
		TableName tableName = TableName.valueOf(tablename);
		
		Admin admin = conn .getAdmin();
		if(admin.tableExists(tableName)) {
			admin.disableTable(tableName);   	//禁用表
			admin.deleteTable(tableName);		//删除表
			
			System.out.println("删除成功");
		}	
		HBaseUtil.close(conn);
		System.out.println("关闭连接");
	}
}
