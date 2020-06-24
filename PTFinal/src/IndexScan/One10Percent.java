package IndexScan;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import Generic.DBConnection;
import Generic.OraRandom;
import Generic.oraSequence;

public class One10Percent {
	
	
	public void createSchema() {
		LoadSmallTable();
		LoadLargeTable();
		createIndexes();
	}
	
	void createIndexes() {
		try {
			Connection oraCon = DBConnection.getOraConn();
			Statement stmt = oraCon.createStatement();
			String SQL = "create index small_mark1_idx on smalltable(mark1)";
			stmt.execute(SQL);
			SQL = "create index large_mark1_idx on largetable(mark1)";
			stmt.execute(SQL);
			
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
	@SuppressWarnings("static-access")
	void LoadSmallTable() {
		try {
			createSmallTable();
			ExecutorService asd = Executors.newFixedThreadPool(10);
			int i = 0;
			while (i < 10) {
				asd.submit(new LoadOnePercent());
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
	
	void LoadLargeTable() {
		try {
			createLargeTable();
			ExecutorService asd = Executors.newFixedThreadPool(10);
			int i = 0;
			while (i < 10) {
				asd.submit(new LoadMorePercent());
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
	
	void createSmallTable() {
		try {
			oraSequence.reset();
			Connection oraCon = DBConnection.getOraConn();
			Statement stmt = oraCon.createStatement();
			String SQL;
			try {
				System.out.println("Dropping an existing table");
				SQL = "drop table smalltable";
				stmt.execute(SQL);
			}
			catch(Exception E) {
				
			}
			SQL = "create table smalltable (student_id number, dept_id number, mark1 number, mark2 number, mark3 number, mark4 number)";
			stmt.execute(SQL);
			System.out.println("Created Table");
			stmt.close();
			oraCon.close();
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
	
	class LoadOnePercent implements Runnable{
		public void run() {
			try {
				Connection oraCon = DBConnection.getOraConn();
				System.out.println("Starting Inserting Thread " + Thread.currentThread().getName());
				PreparedStatement pstmt = oraCon.prepareStatement("insert into smalltable(student_id,dept_id,mark1,mark2,mark3,mark4) values (?,?,?,?,?,?)");
				int i = 0;
				while (i < 1000000) {
					pstmt.setInt(1, oraSequence.nextVal());
					pstmt.setInt(2, OraRandom.randomUniformInt(200));
					pstmt.setInt(3, OraRandom.randomSkewInt(300));
					pstmt.setInt(4, OraRandom.randomSkewInt(1800));
					pstmt.setInt(5,  OraRandom.randomSkewInt(6400));
					pstmt.setInt(6, OraRandom.randomSkewInt(12800));
					pstmt.addBatch();
					if (i%10000 == 0) {
						pstmt.executeBatch();
						System.out.println("Loaded " + oraSequence.getval() + " --> Rows");
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
	
	
	
	void createLargeTable() {
		try {
			oraSequence.reset();
			Connection oraCon = DBConnection.getOraConn();
			Statement stmt = oraCon.createStatement();
			String SQL;
			try {
				System.out.println("Dropping an existing table");
				SQL = "drop table largetable";
				stmt.execute(SQL);
			}
			catch(Exception E) {
				
			}
			SQL = "create table largetable (student_id number, data varchar2(300), dept_id number, mark1 number, mark2 number, mark3 number, mark4 number)";
			stmt.execute(SQL);
			System.out.println("Created Table");
			stmt.close();
			oraCon.close();
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
	
	class LoadMorePercent implements Runnable{
		public void run() {
			try {
				Connection oraCon = DBConnection.getOraConn();
				System.out.println("Starting Inserting Thread " + Thread.currentThread().getName());
				PreparedStatement pstmt = oraCon.prepareStatement("insert into largetable(student_id,data,dept_id,mark1,mark2,mark3,mark4) values (?,?,?,?,?,?,?)");
				int i = 0;
				while (i < 1000000) {
					pstmt.setInt(1, oraSequence.nextVal());
					pstmt.setString(2, OraRandom.randomString(300));
					pstmt.setInt(3, OraRandom.randomUniformInt(200));
					pstmt.setInt(4, OraRandom.randomSkewInt(300));
					pstmt.setInt(5, OraRandom.randomSkewInt(1800));
					pstmt.setInt(6,  OraRandom.randomSkewInt(6400));
					pstmt.setInt(7, OraRandom.randomSkewInt(12800));
					pstmt.addBatch();
					if (i%10000 == 0) {
						pstmt.executeBatch();
						System.out.println("Loaded " + oraSequence.getval() + " --> Rows");
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
}
