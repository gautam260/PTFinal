package Generic;

import java.sql.Statement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class Batching {

	void updateLoad() {
		try {
			Connection oraCon = DBConnection.getOraConn();
			Statement stmt  = oraCon.createStatement();
			String SQL = "select max(student_id) from batching";
			ResultSet rs = stmt.executeQuery(SQL);
			int MAXVALUE = 0;
			while (rs.next()) {
				MAXVALUE = rs.getInt(1);
			}
			int i = 0;
			PreparedStatement pstmt = oraCon.prepareStatement("update batching set mark1 = ? where student_id = ?");
			while (i < 10000000) {
				pstmt.setInt(1, OraRandom.randomUniformInt(200));
				pstmt.setInt(2, OraRandom.randomUniformInt(MAXVALUE));
				pstmt.addBatch();
				if (i%1000 == 0 ) {
					pstmt.executeBatch();
				}
				i++;
			}
			pstmt.executeBatch();
			pstmt.close();
			stmt.close();
			oraCon.close();
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
	
	void deleteLoad() {
		try {
			Connection oraCon = DBConnection.getOraConn();
			Statement stmt  = oraCon.createStatement();
			String SQL = "select max(student_id) from batching";
			ResultSet rs = stmt.executeQuery(SQL);
			int MAXVALUE = 0;
			while (rs.next()) {
				MAXVALUE = rs.getInt(1);
			}
			int i = 0;
			PreparedStatement pstmt = oraCon.prepareStatement("delete from batching where student_id = ?");
			while (i < 10000000) {
				pstmt.setInt(1, OraRandom.randomUniformInt(MAXVALUE));
				pstmt.addBatch();
				if (i%1000 == 0 ) {
					pstmt.executeBatch();
				}
				i++;
			}
			pstmt.executeBatch();
			pstmt.close();
			stmt.close();
			oraCon.close();
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
	
	
	
	void selectLoad() {
		try {
			Connection oraCon = DBConnection.getOraConn();
			Statement stmt  = oraCon.createStatement();
			String SQL = "select max(student_id) from batching";
			ResultSet rs = stmt.executeQuery(SQL);
			int MAXVALUE = 0;
			while (rs.next()) {
				MAXVALUE = rs.getInt(1);
			}
			int i = 0;
			PreparedStatement pstmt = oraCon.prepareStatement("select * from batching where student_id = ?");
			while (i < 10000000) {
				pstmt.setInt(1, OraRandom.randomUniformInt(MAXVALUE));
				pstmt.addBatch();
				if (i%1000 == 0 ) {
					pstmt.executeBatch();
				}
				i++;
			}
			pstmt.executeBatch();
			pstmt.close();
			stmt.close();
			oraCon.close();
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
	
	
	void insertLoad() {
		try {
			Connection oraCon = DBConnection.getOraConn();
			PreparedStatement pstmt = oraCon.prepareStatement("insert into batching(student_id,dept_id,mark1, mark2,mark3) values (?,?,?,?,?)");
			int i = 1;
			while (i < 1000000) {
				pstmt.setInt(1, oraSequence.nextVal());
				pstmt.setInt(2, OraRandom.randomUniformInt(200));
				pstmt.setInt(3, OraRandom.randomUniformInt(800));
				pstmt.setInt(4, OraRandom.randomUniformInt(10000));
				pstmt.setInt(5, OraRandom.randomUniformInt(20000));
				pstmt.addBatch();
				if (i/10000 == 0) {
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
	
	
	
	
	void createTable() {
		try {
			Connection oraCon = DBConnection.getOraConn();
			Statement stmt = oraCon.createStatement();
			String SQL ;
			try {
				SQL = "drop table batching";
				stmt.execute(SQL);
			}
			catch(Exception E) {
				
			}
			SQL = "create table batching(student_id number, dept_id number, mark1 number, mark2 number, mark3 number)";
			stmt.execute(SQL);
			SQL = "alter table batching add constraint batching_pk primary key(student_id)";
			stmt.execute(SQL);
			SQL = "create index mark3_idx on batching(mark3)";
			stmt.execute(SQL);
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
}
