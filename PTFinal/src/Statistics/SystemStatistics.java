package Statistics;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import Generic.DBConnection;
import Generic.OraRandom;
import Generic.oraSequence;

public class SystemStatistics {
	
	public void init() {
		createTable();
		loadTable();
	}
	
	public void run() {
		displayStats();
		gatherStart();
		ExecutorService asd = Executors.newFixedThreadPool(10);
		int i = 0;
		int randValue = OraRandom.randomUniformInt(10);
		while (i < randValue) {
			asd.submit(new IndexLoad());
			i++;
		}
		i = 0;
		while (i < (10 - randValue)) {
			asd.submit(new FullLoad());
			i++;
		}
	}
	
	class IndexLoad implements Runnable{
		public void run() {
			try {
				Connection oraCon = DBConnection.getOraConn();
			//	oraCon.setClientInfo("ApplicationName", "IndexLoad");
				System.out.println("Running IndexLoad " + Thread.currentThread().getName());
				Statement stmt = oraCon.createStatement();
				ResultSet rs = stmt.executeQuery("select max(t1) from systemstats");
				int MAXVALUE = 0;
				while (rs.next()) {
					MAXVALUE = rs.getInt(1);
				}
				int i = 0;
				stmt.close();
				rs.close();
				PreparedStatement pstmt = oraCon.prepareStatement("select avg(length(t2)) from systemstats where t1 = ?");
				while (i < 10000) {
					pstmt.setInt(1, OraRandom.randomUniformInt(MAXVALUE));
					rs = pstmt.executeQuery();
					while (rs.next()) {
						rs.getInt(1);
					}
					rs.close();
					
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
	
	class FullLoad implements Runnable{
		public void run() {
			try {
				Connection oraCon = DBConnection.getOraConn();
			//	oraCon.setClientInfo("ApplicationName", "FullLoad");
				System.out.println("Running FullLoad " + Thread.currentThread().getName());
				ResultSet rs ;
				PreparedStatement pstmt = oraCon.prepareStatement("select avg(length(t2)) from systemstats");
				int i = 0;
				while (i < 10000) {
					rs = pstmt.executeQuery();
					while (rs.next()) {
						rs.getInt(1);
					}
					rs.close();
					
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
			System.out.println("Creating Table");
			String SQL = "create table systemstats(t1 number primary key, t2 varchar2(3000), t3 varchar2(3000))";
			stmt.execute(SQL);
			stmt.close();
			oraCon.close();
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
	
	void loadTable() {
		try {
			Connection oraCon = DBConnection.getOraConn();
			System.out.println("Loading Table");
			String SQL = "insert into systemstats(t1, t2, t3) values (?,?,?)";
			PreparedStatement pstmt = oraCon.prepareStatement(SQL);
			int i = 1 ; 
			while (i < 250000) {
				pstmt.setInt(1, oraSequence.nextVal());
				pstmt.setString(2, OraRandom.randomString(3000));
				pstmt.setString(3, OraRandom.randomString(3000));
				pstmt.addBatch();
				if (i%10000 == 0 ) {
					pstmt.executeBatch();
					System.out.println("Loaded " + i + " rows ");
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
	
	
	void gatherStart() {
		try {
			Connection oraCon = DBConnection.getOraConn();
			CallableStatement cstmt = oraCon.prepareCall ("{call dbms_stats.gather_system_stats('START')}");
			cstmt.executeUpdate();
			oraCon.close();
			System.out.println("Starting System Statistics Gathering!");
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
	
	void gatherStop() {
		try {
			Connection oraCon = DBConnection.getOraConn();
			CallableStatement cstmt = oraCon.prepareCall ("{call dbms_stats.gather_system_stats('STOP')}");
			cstmt.executeUpdate();
			oraCon.close();
			System.out.println("Stopping System Statistics Gathering!");
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
	
	public void displayStats() {
		try {
			Connection oraCon = DBConnection.getOraSysConn();
			Statement stmt = oraCon.createStatement();
			String SQL = "select pname,pval1 from sys.aux_stats$";
			ResultSet rs = stmt.executeQuery(SQL);
			String temp1;
			while(rs.next()) {
				temp1 = rs.getString(1);
				if (temp1.length()!=20) {
					System.out.println(temp1 + OraRandom.spaces(20-temp1.length()) + " --> " + rs.getString(2));
				}
			}
			rs.close();
			stmt.close();
			oraCon.close();
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
	
}
