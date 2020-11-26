package BulkLoad;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MapReduceExtendedCell;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/*
 *BulkLoad是将数据导入的时候可以批量将数据直接生成Hfile，放在HDFS中，避免直接和HBase连接，使用put进行操作
 *		优点：1.绕过之前的写流程（WAL， MemStore， StoreFile合并）
 *			 2.批量写的时候效果高 
 */

public class BulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, MapReduceExtendedCell> {
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, ImmutableBytesWritable, MapReduceExtendedCell>.Context context)
			throws IOException, InterruptedException {
		//1.将Mapper获取到的Text文本行，转换为CSV_Transferecord；实体类
		CSV_TranferRecord transferRecord = CSV_TranferRecord.parse(value.toString());
		
		//2.从实体类获取empno，并转换成rowkey
		String rowkeyString = transferRecord.getEmpno();
		byte[] rowkeyByteArray = Bytes.toBytes(rowkeyString);
		byte[] columnFamily = Bytes.toBytes("C1");	
		byte[] colEmpno = Bytes.toBytes("empno");	//行键
		
		byte[] colEname = Bytes.toBytes("ename");
		byte[] colJob = Bytes.toBytes("job");
		byte[] colMgr = Bytes.toBytes("mgr");
		byte[] colHirdate = Bytes.toBytes("hirdat");
		byte[] colSal = Bytes.toBytes("sal");
		byte[] colComm = Bytes.toBytes("comm");
		byte[] colDeptno = Bytes.toBytes("deptno");
		
		//3.构造Key_Out: new ImmutableBytesWrite(rowkey)
		ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable(rowkeyByteArray);
		
		//4.使用KeyValue构建单元格，每个需要写到表中的字段都需要构建单元格		 
		KeyValue kvEmpno = new KeyValue(rowkeyByteArray, columnFamily, colEmpno, Bytes.toBytes(transferRecord.getEmpno()));
        KeyValue kvEname = new KeyValue(rowkeyByteArray, columnFamily, colEname, Bytes.toBytes(transferRecord.getEname()));
        KeyValue kvJob = new KeyValue(rowkeyByteArray, columnFamily, colJob, Bytes.toBytes(transferRecord.getJob()));
        KeyValue kvMgr = new KeyValue(rowkeyByteArray, columnFamily, colMgr, Bytes.toBytes(transferRecord.getMgr()));
        KeyValue kvHirdate = new KeyValue(rowkeyByteArray, columnFamily, colHirdate, Bytes.toBytes(transferRecord.getHirdate()));
        KeyValue kvSal = new KeyValue(rowkeyByteArray, columnFamily, colSal, Bytes.toBytes(transferRecord.getSal()));
        KeyValue kvComm = new KeyValue(rowkeyByteArray, columnFamily, colComm, Bytes.toBytes(transferRecord.getComm()));
        KeyValue kvDeptno = new KeyValue(rowkeyByteArray, columnFamily, colDeptno, Bytes.toBytes(transferRecord.getDeptno()));
	
        //5.构造输出的value：new MapReduceExtendedCell(keyvalue对象)
        context.write(immutableBytesWritable, new MapReduceExtendedCell(kvEmpno));
        context.write(immutableBytesWritable, new MapReduceExtendedCell(kvEname));
        context.write(immutableBytesWritable, new MapReduceExtendedCell(kvJob));
        context.write(immutableBytesWritable, new MapReduceExtendedCell(kvMgr));
        context.write(immutableBytesWritable, new MapReduceExtendedCell(kvHirdate));
        context.write(immutableBytesWritable, new MapReduceExtendedCell(kvSal));
        context.write(immutableBytesWritable, new MapReduceExtendedCell(kvComm));
        context.write(immutableBytesWritable, new MapReduceExtendedCell(kvDeptno));
	}
	
}
