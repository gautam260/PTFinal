package IndexScan;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import Generic.DBConnection;
import Generic.OraRandom;
import Generic.oraSequence;
import Statistics.GatherStats;

public class UniqueNonunique {

	
	
	@SuppressWarnings("static-access")
	public void run() {
		try {
			ExecutorService asd = Executors.newFixedThreadPool(20);
			int i = 0;
			while (i < 10) {
				asd.submit(new UniqueLoad());
				asd.submit(new NonUniqueLoad());
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
	
	
	class UniqueLoad implements Runnable{
		public void run() {
			try {
				Connection oraCon = DBConnection.getOraConn();
				Statement stmt = oraCon.createStatement();
				String SQL = "select * from unique1 where student_id = 25231";
				int i = 0 ;
				ResultSet rs;
				while (i < 10000) {
					rs = stmt.executeQuery(SQL);
					while(rs.next()) {
						rs.getInt(3);
					}
					i++;
					rs.close();
				}
				
				stmt.close();
				oraCon.close();
				System.out.println("UniqueLoad " + Thread.currentThread().getName() + " --> Complete");
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
	}
	
	class NonUniqueLoad implements Runnable{
		public void run() {
			try {
				Connection oraCon = DBConnection.getOraConn();
				Statement stmt = oraCon.createStatement();
				String SQL = "select * from nonunique where student_id = 25231";
				int i = 0 ;
				ResultSet rs;
				while (i < 50000) {
					rs = stmt.executeQuery(SQL);
					while(rs.next()) {
						rs.getInt(4);
					}
					i++;
					rs.close();
				}
				
				stmt.close();
				oraCon.close();
				System.out.println("Non-UniqueLoad " + Thread.currentThread().getName() + " --> Complete");
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
	}
	
	public void init() {
		createUnique();
		createNonUnique();
		loadTable();
		GatherStats a = new GatherStats("UNIQUE1");
		a.run();
		GatherStats b = new GatherStats("NONUNIQUE");
		b.run();
	}
	
	
	void loadTable() {
		try {

			Connection oraCon = DBConnection.getOraConn();
			System.out.println("Starting Inserting Thread " + Thread.currentThread().getName());
			PreparedStatement pstmt = oraCon.prepareStatement("insert into nonunique(student_id,dept_id,mark1,mark2,mark3,mark4) values (?,?,?,?,?,?)");
			PreparedStatement pstmt2 = oraCon.prepareStatement("insert into unique1(student_id,dept_id,mark1,mark2,mark3,mark4) values (?,?,?,?,?,?)");
			int i = 0;
			while (i < 1000000) {
				pstmt.setInt(1, oraSequence.nextVal());
				pstmt.setInt(2, OraRandom.randomUniformInt(200));
				pstmt.setInt(3, OraRandom.randomSkewInt(300));
				pstmt.setInt(4, OraRandom.randomSkewInt(1800));
				pstmt.setInt(5,  OraRandom.randomSkewInt(6400));
				pstmt.setInt(6, OraRandom.randomSkewInt(12800));
				pstmt.addBatch();
				pstmt2.setInt(1, oraSequence.getval());
				pstmt2.setInt(2, OraRandom.randomUniformInt(200));
				pstmt2.setInt(3, OraRandom.randomSkewInt(300));
				pstmt2.setInt(4, OraRandom.randomSkewInt(1800));
				pstmt2.setInt(5,  OraRandom.randomSkewInt(6400));
				pstmt2.setInt(6, OraRandom.randomSkewInt(12800));
				pstmt2.addBatch();
				if (i%10000 == 0) {
					pstmt.executeBatch();
					pstmt2.executeBatch();
					System.out.println("Loaded " + oraSequence.getval() + " --> Rows");
				}
				i++;
			}
			pstmt2.executeBatch();
			pstmt.executeBatch();
			pstmt.close();
			pstmt2.close();
			oraCon.close();
				
		
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
	
	void createNonUnique() {
		try {
			oraSequence.reset();
			Connection oraCon = DBConnection.getOraConn();
			Statement stmt = oraCon.createStatement();
			String SQL;
			try {
				System.out.println("Dropping an existing table");
				SQL = "drop table nonunique";
				stmt.execute(SQL);
			}
			catch(Exception E) {
				
			}
			SQL = "create table nonunique (student_id number, dept_id number, mark1 number, mark2 number, mark3 number, mark4 number)";
			stmt.execute(SQL);
			SQL = "create index nonunique_idx on nonunique(Student_id)";
			stmt.execute(SQL);
			System.out.println("Created Table");
			stmt.close();
			oraCon.close();
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
	void createUnique() {
		try {
			oraSequence.reset();
			Connection oraCon = DBConnection.getOraConn();
			Statement stmt = oraCon.createStatement();
			String SQL;
			try {
				System.out.println("Dropping an existing table");
				SQL = "drop table unique1";
				stmt.execute(SQL);
			}
			catch(Exception E) {
				
			}
			SQL = "create table unique1(student_id number, dept_id number, mark1 number, mark2 number, mark3 number, mark4 number)";
			stmt.execute(SQL);
			SQL = "create unique index unique1_idx on unique1(Student_id)";
			stmt.execute(SQL);
			System.out.println("Created Table");
			stmt.close();
			oraCon.close();
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
}
