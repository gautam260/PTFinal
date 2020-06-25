package Statistics;
import java.sql.CallableStatement;
import java.sql.Connection;

import Generic.DBConnection;

public class GatherStats {
	String tabName;
	public GatherStats(String table){
		this.tabName = table;
	}
	public void run() {

		try {
			Connection oraCon = DBConnection.getOraConn();
			CallableStatement cstmt = oraCon.prepareCall ("{call dbms_stats.gather_table_stats('VISHNU',?,cascade=>true, no_invalidate=>false)}");
			cstmt.setString(1, tabName);
			cstmt.executeUpdate();
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
}
