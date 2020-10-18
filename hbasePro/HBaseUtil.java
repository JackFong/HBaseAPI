package Util;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseUtil {
	private HBaseUtil() {}		
	
	//----------------------创建链接对象----------------------
	public static Connection getHBaseConnection() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		
		return ConnectionFactory.createConnection(conf);
	}
	
	//------------------------创建表------------------------
	public static void createTable(String tablename,HTableDescriptor td)throws Exception{		
		Configuration conf = HBaseConfiguration.create();
		Connection conn = HBaseUtil.getHBaseConnection();
		Admin admin = conn.getAdmin();
		System.out.print("请输入该表要添加列族的名称：");
		Scanner b = new Scanner(System.in);
		String family = b.nextLine();
		td.addFamily(new HColumnDescriptor(family));
		
		System.out.print("是否继续添加列族（y or n）：");		
		Scanner c = new Scanner(System.in);
		String select = c.nextLine();
		
		boolean flag = true;
		while(flag){
			if(select.equals("y")) {
				HBaseUtil.createTable(tablename,td);		//继续调用新建列族函数
				break;
				
			}else if(select.equals("n")) {				
				admin.createTable(td);		//执行admin.crateTable(new HTableDescriptor(tablName));
				admin.close();			//关闭admin意味着创建表格结束
				
				flag = false;		//更改flag，跳出循环
				System.out.println("创建成功!");
				return;
			}else {
				System.out.println(flag);
				System.out.println("没有更多选择！");
				continue;//继续调用新建列族函数
			}
		}
	}	
	
		
	//---------------------插入数据--------------------------
	public static void insertData(String tablename) throws Exception{			//String tableName, String rowkey, String family,String column,String value
		Connection conn = getHBaseConnection();
		Admin admin = conn.getAdmin();
		Table table = conn.getTable(TableName.valueOf(tablename));
		System.out.print("请输入要往表中插入数据的行键：");
		Scanner b = new Scanner(System.in);
		String rowkey = b.nextLine();		
		
		//判断所输入的列族是否存在
		boolean flag = true;
		while(flag) {
			System.out.print("请输入该行键对应的列族的名称：");
			Scanner c = new Scanner(System.in);
			String family = c.nextLine();
			TableDescriptor tableDescriptor = table.getDescriptor();
			ColumnFamilyDescriptor descriptor = tableDescriptor.getColumnFamily(Bytes.toBytes(family));
			
			if(descriptor == null) {				
				System.out.println("该列族不存在，无法插入数据！");
				continue;				
			}else {				
				System.out.print("请输入该行键对应的列的名称：");
				Scanner d = new Scanner(System.in);
				String column = d.nextLine();
				
				System.out.print("请输入要插入该位置的值：");
				Scanner e = new Scanner(System.in);
				String value = e.nextLine();
				
				Put put = new Put(Bytes.toBytes(rowkey));		//把字符串以UTF-8转换成二进制
				put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
				table.put(put);

				boolean flag2 = true;
				while(flag2) {
					System.out.print("是否继续插入数据（y or n）");
					Scanner f = new Scanner(System.in);
					String select = f.nextLine();
					if(select.equals("y")) {
						HBaseUtil.insertData(tablename);
						break;						
					}else if(select .equals("n")) {						
						System.out.println("取消插入");
						flag = false;
						flag2 = false;
						return;						
					}else {						
						System.out.println("没有更多选择！");	
						continue;
					}
				}
				break;
			}
		}
		admin.close();
		table.close();
	}
	
	//---------------------关闭链接---------------------
	public static void close(Connection conn)throws Exception{
		if(conn != null) {
			conn.close();
		}
	}


}
