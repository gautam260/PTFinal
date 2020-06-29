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
			CallableStatement cstmt = oraCon.prepareCall ("{call dbms_stats.gather_table_stats('VISHNU',?, ESTIMATE_PERCENT=>100, CASCADE=> TRUE, METHOD_OPT=>'FOR ALL COLUMNS SIZE AUTO', GRANULARITY=>'ALL', DEGREE=> 8)}");
			cstmt.setString(1, tabName);
			cstmt.executeUpdate();
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
}
