package BulkLoad;

import org.apache.hadoop.classification.InterfaceAudience.Public;

public class CSV_TranferRecord {	//构建实体类
	private static final CSV_TranferRecord CSV_TransferRecord = null;
	private String empno;
	private String ename;
	private String job;
	private String mgr;
	private String hirdate;
	private String sal;
	private String comm;
	private String deptno;
	
	
	public String getEmpno() {
		return empno;
	}
	public void setEmpno(String empno) {
		this.empno = empno;
	}
	public String getEname() {
		return ename;
	}
	public void setEname(String ename) {
		this.ename = ename;
	}
	public String getJob() {
		return job;
	}
	public void setJob(String job) {
		this.job = job;
	}
	public String getMgr() {
		return mgr;
	}
	public void setMgr(String mgr) {
		this.mgr = mgr;
	}
	public String getHirdate() {
		return hirdate;
	}
	public void setHirdate(String hirdate) {
		this.hirdate = hirdate;
	}
	public String getSal() {
		return sal;
	}
	public void setSal(String sal) {
		this.sal = sal;
	}
	public String getComm() {
		return comm;
	}
	public void setComm(String comm) {
		this.comm = comm;
	}
	public String getDeptno() {
		return deptno;
	}
	public void setDeptno(String deptno) {
		this.deptno = deptno;
	}
	@Override
	public String toString() {
		return "CSV_TranferRecord [empno=" + empno + ", ename=" + ename + ", job=" + job + ", mgr=" + mgr + ", hirdate="
				+ hirdate + ", sal=" + sal + ", comm=" + comm + ", deptno=" + deptno + "]";
	}
	
	//编写MR程序时，需要获取CSV文件的一行
	//通过调用parse方法就可以将一行逗号分隔的文本解析为TransferRecord
	public static CSV_TranferRecord parse(String line) {
		String[] fieldsArray = line.split(",");
		
		CSV_TranferRecord transferRecord = new CSV_TranferRecord();
		transferRecord.setEmpno(fieldsArray[0]);  
		transferRecord.setEname(fieldsArray[1]);  
		transferRecord.setJob(fieldsArray[2]);    
		transferRecord.setMgr(fieldsArray[3]);
		transferRecord.setHirdate(fieldsArray[4]);
		transferRecord.setSal(fieldsArray[5]); 
		transferRecord.setComm(fieldsArray[6]);   
		transferRecord.setDeptno(fieldsArray[7]);
		
		return transferRecord;
	}
	
//	public static void main(String[] args) {
//		String trStr = "7369,SMITH,CLERK,7902,1980/12/17,800,,20";
//		System.out.println(CSV_TransferRecord.parse(trStr));
//	}
	
}   
