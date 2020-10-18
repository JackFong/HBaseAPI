package com.study.hbasePro;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseRead_All {
	public static void main(String[] args) throws IOException {
		// 配置对象，获取hbase的连接
		Configuration conf = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(conf);
		System.out.println("成功连接至hbase");
		System.out.println("--------查询数据（全表）--------");
		
		// 获取操作对象：Admin
		Admin admin = connection.getAdmin();
		
		System.out.print("请输入要查询的hbase表：");		// 判断hbase中是否存在某张表
		Scanner a = new Scanner(System.in);
		String tablename = a.nextLine();
		TableName tableName = TableName.valueOf(tablename);
		
		boolean flag = admin.tableExists(tableName);		
		if (flag) {
			// 获取指定的表对象
			Table table = connection.getTable(tableName);
			// 查询数据
			Scan scanner = new Scan(); 
			ResultScanner rs = table.getScanner(scanner);

			System.out.println("[ 行键，    列族，    列，    值 ]");
            for (Result r : rs){
            	for(Cell k : r.rawCells()) {
            		System.out.print("[ " + Bytes.toString(CellUtil.cloneRow(k)) + ",");			//转换成字符串输出               
					System.out.print("    " + Bytes.toString(CellUtil.cloneFamily(k)) + ",");
	                System.out.print("    " + Bytes.toString(CellUtil.cloneQualifier(k)) + ",");
	                System.out.println("    " + Bytes.toString(CellUtil.cloneValue(k)) + " ]");
            	}
            }
			
        }else {
        	System.out.println("该表不存在！");	
        }
		admin.close();
	}
}
