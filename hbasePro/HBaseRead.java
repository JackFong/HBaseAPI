package com.study.hbasePro;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseRead {
	public static void main(String[] args) throws IOException {
		// 配置对象，获取hbase的连接
		Configuration conf = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(conf);
		System.out.println("成功连接至hbase");
		System.out.println("--------查询数据（行）--------");
		
		// 获取操作对象：Admin
		Admin admin = connection.getAdmin();
		
		// 操作数据库：	
		// 判断hbase中是否存在某张表
		System.out.print("请输入要查询的hbase表：");
		Scanner a = new Scanner(System.in);
		String tablename = a.nextLine();
		TableName tableName = TableName.valueOf(tablename);
		
		boolean flag = admin.tableExists(tableName);		
		if (flag) {
			// 获取指定的表对象
			Table table = connection.getTable(tableName);
			// 查询数据
			System.out.print("请输入要查询的行键：");
			Scanner b = new Scanner(System.in);
			String rowkey = b.nextLine();
			
            Get get = new Get(Bytes.toBytes(rowkey));
            Result result = table.get(get);
            boolean empty = result.isEmpty();
            // 展示数据
            if(empty) {
            	System.out.println("该行键不存在");
            }else {
            	System.out.println("[ 行键，    列族，    列，    值 ]");
	            for (Cell cell : result.rawCells()){      
	                System.out.print("[ " + Bytes.toString(CellUtil.cloneRow(cell)) + ",");			//转换成字符串输出
	                System.out.print("    " + Bytes.toString(CellUtil.cloneFamily(cell)) + ",");
	                System.out.print("    " + Bytes.toString(CellUtil.cloneQualifier(cell)) + ",");
	                System.out.println("    " + Bytes.toString(CellUtil.cloneValue(cell)) + " ]");
	            }
            }
        }else {
        	System.out.println("该表不存在！");
        	
        }
		admin.close();
	}
}
