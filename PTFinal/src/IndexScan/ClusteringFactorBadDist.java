package IndexScan;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import Generic.DBConnection;
import Generic.OraRandom;
import Generic.oraSequence;

public class ClusteringFactorBadDist {

	
	
	@SuppressWarnings("static-access")
	public void LoadData() {
		try {

			createTable();
			
			int i = 0;
			ExecutorService asd = Executors.newFixedThreadPool(1);
			while (i < 12000) {
				if (i%2==0) {
					asd.submit(new loadDataSingle(i));
				}
				i++;
			}
			asd.shutdown();
			while(!asd.isTerminated()) {
				Thread.currentThread().sleep(1000);
				System.out.print(".");
			}
			asd.shutdownNow();
			i = 0;
			asd = Executors.newFixedThreadPool(50);
			while (i < 12000) {
				if (i%2==1) {
					asd.submit(new loadDataMultiple(i));
				}
				i++;
			}
			asd.shutdown();
			while(!asd.isTerminated()) {
				Thread.currentThread().sleep(1000);
				System.out.print(".");
			}
			
			
			
		
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
	
	void createTable() {
		try {
			Connection oraCon = DBConnection.getOraConn();
			Statement stmt = oraCon.createStatement();
			String SQL;
			try {
				SQL = "drop table Baddist";
				stmt.execute(SQL);
			}
			catch(Exception E) {
			}
			SQL = "create table Baddist (student_id number, name varchar2(30), dept_id number,  marks number)";
			stmt.execute(SQL);
			SQL = "alter table Baddist add constraint clustering_pk primary key(student_id)";
			stmt.execute(SQL);
			stmt.close();
			oraCon.close();
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}

	
	class loadDataSingle implements Runnable{
		int marks;
		loadDataSingle (int a ){
			this.marks = a;
		}
		public void run() {
			try {
				System.out.println("Starting single ==> " + marks);
				Connection oraCon = DBConnection.getOraConn();
				PreparedStatement pstmt = oraCon.prepareStatement("insert into Baddist (student_id, name, dept_id, marks) values (?,?,?,?)");
				int i  = 1;
				while (i < 2000) {
					pstmt.setInt(1, oraSequence.nextVal());
					pstmt.setString(2, OraRandom.randomString(30));
					pstmt.setInt(3, OraRandom.randomUniformInt(100));
					pstmt.setInt(4, marks);
					pstmt.addBatch();
					i++;
				}
				pstmt.executeBatch();
				pstmt.close();
				oraCon.close();
				System.out.println("Completed single ==> " + marks);
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
	}
	
	
	
	class loadDataMultiple implements Runnable{
		int marks;
		loadDataMultiple (int a ){
			this.marks = a;
		}
		public void run() {
			try {
				System.out.println("Starting multiple ==> " + marks);
				Connection oraCon = DBConnection.getOraConn();
				PreparedStatement pstmt = oraCon.prepareStatement("insert into Baddist (student_id, name, dept_id, marks) values (?,?,?,?)");
				int i  = 1;
				while (i < 2000) {
					pstmt.setInt(1, oraSequence.nextVal());
					pstmt.setString(2, OraRandom.randomString(30));
					pstmt.setInt(3, OraRandom.randomUniformInt(100));
					pstmt.setInt(4, marks);
					pstmt.executeUpdate();
					i++;
				}
				pstmt.close();
				oraCon.close();
				System.out.println("Completed multiple ==> " + marks);
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
	}
}
