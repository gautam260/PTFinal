package IndexScan;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import Generic.DBConnection;
import Generic.OraRandom;
import Generic.oraSequence;

public class ClusteringFactor {


	@SuppressWarnings("static-access")
	public void run() {
		try {
			createTableASSM();
			ExecutorService asd = Executors.newFixedThreadPool(10);
			int i = 0;
			while (i < 10) {
					asd.submit(new classicInsertSEQ());
				i++;
			}
			asd.shutdown();
			while(!asd.isTerminated()) {
				Thread.currentThread().sleep(1000);
				System.out.print(".");
			}
			asd.shutdownNow();
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
	
	class BatchInsertSEQ implements Runnable{
		public void run() {
			try {
				System.out.println("Running Batch Insert oraSEQ --> " + Thread.currentThread().getName());
				Connection oraCon = DBConnection.getOraConn();
				PreparedStatement pstmt = oraCon.prepareStatement("insert into students(student_id, name, dept_id, marks) values (ora.nextval,?,?,?)");
				int i = 1;
				while (i < 100000) {
					pstmt.setString (1, OraRandom.randomString(30));
					pstmt.setInt(2, OraRandom.randomUniformInt(100));
					pstmt.setInt(3, OraRandom.randomUniformInt(100));
					pstmt.addBatch();
					if (i%100 == 0){
						pstmt.executeBatch();
					}
					i++;
				}
				pstmt.executeBatch();
				pstmt.close();
				oraCon.close();
				System.out.println("Running Batch Insert oraSEQ --> " + Thread.currentThread().getName());
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
	}
	
	class classicInsertSEQ implements Runnable{
		public void run() {
			try {
				System.out.println("Running classic Insert oraSEQ --> " + Thread.currentThread().getName());
				Connection oraCon = DBConnection.getOraConn();
				PreparedStatement pstmt = oraCon.prepareStatement("insert into students(student_id, name, dept_id, marks) values (ora.nextval,?,?,?)");
				int i = 1;
				while (i < 100000) {
					pstmt.setString (1, OraRandom.randomString(30));
					pstmt.setInt(2, OraRandom.randomUniformInt(100));
					pstmt.setInt(3, OraRandom.randomUniformInt(100));
					pstmt.executeUpdate();
					i++;
				}
				pstmt.close();
				oraCon.close();
				System.out.println("Running classic Insert oraSEQ --> " + Thread.currentThread().getName());
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
	}

	class BatchInsert implements Runnable{
		public void run() {
			try {
				System.out.println("Running Batch Insert --> " + Thread.currentThread().getName());
				Connection oraCon = DBConnection.getOraConn();
				PreparedStatement pstmt = oraCon.prepareStatement("insert into students(student_id, name, dept_id, marks) values (?,?,?,?)");
				int i = 1;
				while (i < 100000) {
					pstmt.setInt(1, oraSequence.nextVal());
					pstmt.setString (2, OraRandom.randomString(30));
					pstmt.setInt(3, OraRandom.randomUniformInt(100));
					pstmt.setInt(4, OraRandom.randomUniformInt(100));
					pstmt.addBatch();
					if (i%100 == 0){
						pstmt.executeBatch();
					}
					i++;
				}
				pstmt.executeBatch();
				pstmt.close();
				oraCon.close();
				System.out.println("Finished Batch Insert --> " + Thread.currentThread().getName());
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
	}
	
	class classicInsert implements Runnable{
		public void run() {
			try {
				Connection oraCon = DBConnection.getOraConn();
				System.out.println("Running classic Insert --> " + Thread.currentThread().getName());
				PreparedStatement pstmt = oraCon.prepareStatement("insert into students(student_id, name, dept_id, marks) values (?,?,?,?)");
				int i = 1;
				while (i < 100000) {
					pstmt.setInt(1, oraSequence.nextVal());
					pstmt.setString (2, OraRandom.randomString(30));
					pstmt.setInt(3, OraRandom.randomUniformInt(100));
					pstmt.setInt(4, OraRandom.randomUniformInt(100));
					pstmt.executeUpdate();
					i++;
				}
				pstmt.close();
				oraCon.close();
				System.out.println("Finished classic Insert --> " + Thread.currentThread().getName());
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
	}
	
	void createTableASSM() {
		try {
			Connection oraCon = DBConnection.getOraConn();
			Statement stmt = oraCon.createStatement();
			String SQL ;
			try {
				SQL = "drop table students";
				stmt.execute(SQL);
			}
			catch(Exception E) {}
			try {
				SQL = "drop sequence ora";
				stmt.execute(SQL);
			}
			catch(Exception E) {}
			SQL = "create table students(student_id number primary key, name varchar2(30), dept_id number, marks number)";
			stmt.execute(SQL);
			SQL = "create sequence ora start with 1 increment by 1 cache 100";
			stmt.execute(SQL);
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
	
	
	void createTableMSSM() {
		try {
			Connection oraCon = DBConnection.getOraConn();
			Statement stmt = oraCon.createStatement();
			String SQL ;
			try {
				SQL = "drop table students";
				stmt.execute(SQL);
			}
			catch(Exception E) {}
			try {
				SQL = "drop sequence ora";
				stmt.execute(SQL);
			}
			catch(Exception E) {}
			SQL = "create table students(student_id number primary key, name varchar2(30), dept_id number, marks number) tablespace MSSM";
			stmt.execute(SQL);
			SQL = "create sequence ora start with 1 increment by 1";
			stmt.execute(SQL);
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
}
