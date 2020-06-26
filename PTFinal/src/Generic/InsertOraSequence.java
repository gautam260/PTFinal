package Generic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class InsertOraSequence {

	
	
	
	@SuppressWarnings("static-access")
	public void run() {
		try {
			ExecutorService asd = Executors.newFixedThreadPool(20);
			int i = 0;
			while (i < 20) {
				asd.submit(new LoadTableClassic());
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
	
	public void init() {
		createTableMoreFreeLists();
	}
	
	
	class LoadTableClassic implements Runnable{
		public void run() {
			try {
				System.out.println("Starting Insert load ==> " +Thread.currentThread().getName() );
				Connection oraCon = DBConnection.getOraConn();
				PreparedStatement pstmt = oraCon.prepareStatement("insert into students(student_id, name) values (ora.nextval,?)");
				int i = 0;
				while (i < 10000) {
					pstmt.setString(1, OraRandom.randomString(30));
					pstmt.executeUpdate();
					i++;
				}
				pstmt.close();
				oraCon.close();
				System.out.println("Insert Load Complete ==> " + Thread.currentThread().getName());
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
	}
	
	void createTableMoreFreeLists() {
		try {
			Connection oraCon = DBConnection.getOraConn();
			Statement stmt = oraCon.createStatement();
			String SQL;
			try {
				SQL = "drop table students";
				stmt.execute(SQL);
			}
			catch(Exception E) {
				
			}
			SQL = "create table students (student_id number primary key, name varchar2(30)) storage (freelists 2)";
			stmt.execute(SQL);
			stmt.close();
			oraCon.close();
			System.out.println("Created Table");
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
				SQL = "drop table students";
				stmt.execute(SQL);
			}
			catch(Exception E) {
				
			}
			SQL = "create table students (student_id number primary key, name varchar2(30))";
			stmt.execute(SQL);
			stmt.close();
			oraCon.close();
			System.out.println("Created Table");
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
}
