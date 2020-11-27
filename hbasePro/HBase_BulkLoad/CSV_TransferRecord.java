package BulkLoad;

//import org.apache.hadoop.classification.InterfaceAudience.Public;

public class CSV_TranferRecord {	//构建实体类

	private int empno;
	private String ename;
	private String job;
	private int mgr;
	private String hiredate;
	private int sal;
	private int comm;
	private int deptno;

	@Override
	public String toString() {
		return empno + "\t" + ename + "\t" + job + "\t" + mgr + "\t" + hiredate + "\t" + sal + "\t" + comm 
                + "\t" + deptno;
	}
	
	public int getEmpno() {
		return empno;
	}

	public void setEmpno(int empno) {
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

	public int getMgr() {
		return mgr;
	}

	public void setMgr(int mgr) {
		this.mgr = mgr;
	}

	public String getHiredate() {
		return hiredate;
	}

	public void setHiredate(String hiredate) {
		this.hiredate = hiredate;
	}

	public int getSal() {
		return sal;
	}

	public void setSal(int sal) {
		this.sal = sal;
	}

	public int getComm() {
		return comm;
	}

	public void setComm(int comm) {
		this.comm = comm;
	}

	public int getDeptno() {
		return deptno;
	}

	public void setDeptno(int deptno) {
		this.deptno = deptno;
	}

//	public static CSV_TranferRecord getCsvTransferrecord() {
//		return CSV_TransferRecord;
//	}


	//编写MR程序时，需要获取CSV文件的一行
	//通过调用parse方法就可以将一行逗号分隔的文本解析为TransferRecord
	public static CSV_TranferRecord parse(String line) {
		String[] fieldsArray = line.split(",");
		
		CSV_TranferRecord transferRecord = new CSV_TranferRecord();
		transferRecord.setEmpno(Integer.parseInt(fieldsArray[0]));  
		transferRecord.setEname(fieldsArray[1]);  
		transferRecord.setJob(fieldsArray[2]);    
		transferRecord.setMgr(Integer.parseInt(fieldsArray[3]));
		transferRecord.setHiredate(fieldsArray[4]);
		transferRecord.setSal(Integer.parseInt(fieldsArray[5])); 
		transferRecord.setComm(Integer.parseInt(fieldsArray[6]));   
		transferRecord.setDeptno(Integer.parseInt(fieldsArray[7]));
		
		return transferRecord;
	}
	public static void main(String[] args) {	//测试
		String trStr = "7369,SMITH,CLERK,7902,1980/12/17,800,0,20";
		System.out.println(CSV_TranferRecord.parse("trSrt"));
	}
	
}   



