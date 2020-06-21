package Statistics;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;

import Generic.DBConnection;
import Generic.OraRandom;
import Generic.oraSequence;

public class SystemStatistics {
	
	public void init() {
		createTable();
		loadTable();
	}
	
	
	void createTable() {
		try {
			Connection oraCon = DBConnection.getOraConn();
			Statement stmt = oraCon.createStatement();
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
			String SQL = "insert into systemstats(t1, t2, t3) values (?,?,?)";
			PreparedStatement pstmt = oraCon.prepareStatement(SQL);
			int i = 1 ; 
			while (i < 200000) {
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
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
}
