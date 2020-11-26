package BulkLoad;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.MapReduceExtendedCell;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * 运行完，需要把输出的hdfs文件导入至HBase（终端执行）
 * hbase org.apache.hadoop.hbase.tool.LoadIncrementalHFiles /hbase/BulkLoad/output EMP(表名)
 */
public class BulkLoadDriver {
	public static void main(String[] args) throws Exception {
		//1.加载配置文件
		Configuration conf = HBaseConfiguration.create();
		
		//2.创建HBase连接
		Connection conn = ConnectionFactory.createConnection(conf);
		
		//3.获取HTable
		Table table = conn.getTable(TableName.valueOf("EMP"));
		
		//构建MR Job
			//4.1 构造Job对象
		Job job = Job.getInstance(conf);//需要传加载的配置的文件
			//4.2 调用setJarByClass, 设置要执行JAR包的class
		job.setJarByClass(BulkLoadDriver.class);
			//4.3 指定接口实现类  默认为TextInputFormat.class 这里可以不用指定
//		job.setInputFormatClass(FileInputFormat.class);
			//4.4 设置MapperClass
		job.setMapperClass(BulkLoadMapper.class);
			//4.5 设置输出键 Output Key Class
		job.setOutputKeyClass(ImmutableBytesWritable.class);
			//4.6 设置输出值 Output Value Class
		job.setOutputValueClass(MapReduceExtendedCell.class);
			//4.7 设置输入输出到HDFS的路径
		FileInputFormat.setInputPaths(job, new Path("hdfs://node1:8020/hbase/BulkLoad/input"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://node1:8020/hbase/BulkLoad/output"));
			//4.8 构造区域查找器，获取HBase Region的分布情况
		RegionLocator regionLocator = conn.getRegionLocator(TableName.valueOf("EMP"));
			//4.9 配置Hfile的输出， （参数： Job, Table, RegionLocator）
		HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
		
		//5.执行任务
		job.waitForCompletion(true);
		
		conn.close();
	}
}
