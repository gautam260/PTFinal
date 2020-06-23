package FullScan;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import Generic.DBConnection;
import Generic.OraRandom;
import Generic.oraSequence;

public class ChainedRows {

	
	public void LoadTable() {
		createTable();
			ExecutorService asd = Executors.newFixedThreadPool(10);
			int i = 0;
			while (i < 10) {
				asd.submit(new InsertData());
				i++;
			}
	}
	
	void createTable() {
		try {
			Connection oraCon = DBConnection.getOraConn();
			Statement stmt = oraCon.createStatement();
			try {
				System.out.println("Dropping table if any");
				stmt.execute("drop table chained_rows");
			}
			catch(Exception E) {
				System.out.println("Table not present");
			}
			stmt.execute("create table chained_rows(t1 number primary key, t2 varchar2(3000), t3 varchar2(3000), t4 varchar2(3000))");
			stmt.close();
			oraCon.close();
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
	
	
	
	
	class InsertData implements Runnable{
		public void run() {
			try {
				Connection oraCon = DBConnection.getOraConn();
				System.out.println("Loading Data --> " + Thread.currentThread().getName());
				PreparedStatement pstmt = oraCon.prepareStatement("insert into chained_rows (t1, t2, t3, t4) values (?,?,?,?)");
				int i = 1;
				while (i < 10000) {
					pstmt.setInt(1, oraSequence.nextVal());
					pstmt.setString(2, OraRandom.randomString(3000));
					pstmt.setString(3, OraRandom.randomString(3000));
					pstmt.setString(4, OraRandom.randomString(3000));
					pstmt.addBatch();
					if (i%1000 == 0 ) {
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
}
