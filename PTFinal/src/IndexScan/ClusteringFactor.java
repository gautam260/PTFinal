package IndexScan;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import Generic.DBConnection;
import Generic.OraRandom;

public class ClusteringFactor {

	@SuppressWarnings("static-access")
	public void runBatchLoad() {
		try {
			createTable();
			ExecutorService asd = Executors.newFixedThreadPool(10);
			int i = 0;
			while (i < 10) {
				asd.submit(new BatchLoad());
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
	
	@SuppressWarnings("static-access")
	public void runClassicLoad() {
		try {
			createTable();
			ExecutorService asd = Executors.newFixedThreadPool(10);
			int i = 0;
			while (i < 10) {
				asd.submit(new ClassicLoad());
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
	
	class BatchLoad implements Runnable{
		public void run() {
			try {
				Connection oraCon = DBConnection.getOraConn();
				PreparedStatement pstmt = oraCon.prepareStatement("insert into clustering(student_id, dept_id, marks) values (ora.nextval, ?,?)");
				int i = 0;
				while (i < 50000) {
					pstmt.setInt(1, OraRandom.randomUniformInt(100));
					pstmt.setInt(2, OraRandom.randomSkewInt(100));
					pstmt.addBatch();
					if (i/100 == 0) {
						pstmt.executeBatch();
					}
					i++;
				}
				pstmt.executeBatch();
				pstmt.close();
				oraCon.close();
				
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
	}
	
	class ClassicLoad implements Runnable{
		public void run() {
			try {
				
				Connection oraCon = DBConnection.getOraConn();
				PreparedStatement pstmt = oraCon.prepareStatement("insert into clustering(student_id, dept_id, marks) values (ora.nextval, ?,?)");
				int i = 0;
				while (i < 50000) {
					pstmt.setInt(1, OraRandom.randomUniformInt(100));
					pstmt.setInt(2, OraRandom.randomSkewInt(100));
					pstmt.executeUpdate();
					i++;
				}
				pstmt.close();
				oraCon.close();
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
	}
	
	void createTable() {
		try {
			Connection oraCon = DBConnection.getOraConn();
			Statement stmt = oraCon.createStatement();
			String SQL;
			try {
				SQL = "drop table clustering";
				stmt.execute(SQL);
			}
			catch(Exception E) {}
			try {
				SQL = "drop sequence ora";
				stmt.execute(SQL);
			}
			catch(Exception E) {}
			SQL = "create sequence ora start with 1 increment by 1";
			stmt.execute(SQL);
			SQL = "create table clustering (student_id number, dept_id number, marks number)";
			stmt.execute(SQL);
			SQL = "alter table clustering add constraint clustering_pk primary key(student_id)";
			stmt.execute(SQL);
			stmt.close();
			oraCon.close();
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
}
