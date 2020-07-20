package IndexScan;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import Generic.DBConnection;

public class IndexCreation {
	int sid;
	public IndexCreation(int a){
		this.sid = a;
	}
	@SuppressWarnings("static-access")
	public void run() {
		try {
		//	
			Connection oraCon = DBConnection.getOraConn();
			Statement stmt = oraCon.createStatement();
			
			String SQL = "select a.username, w.event,e.object_name, nvl(to_char(round(((b.blocks*p.value)/1024/1024),2),999999),'0') ,round(c.PGA_USED_MEM/1024/1024) , d.value/1024/1024 from v$session a, (select session_addr,sum(blocks) blocks from v$sort_usage group by session_addr) b, v$process c, v$parameter p, v$session_wait w, v$sesstat d, dba_objects e where p.name='db_block_size' and a.saddr = b.session_addr(+) and a.sid=w.sid(+) and a.sid=d.sid and d.statistic#=313 and a.ROW_WAIT_OBJ#=e.object_id and a.paddr=c.addr  and a.username is not null and a.sid = "+ Integer.toString(sid);
			int i = 0;
			while (i < 10000) {
				ResultSet rs = stmt.executeQuery(SQL);
				while(rs.next()) {
					System.out.print("User -> " + rs.getString(1) );
					System.out.print(" ----> EVENT ----> " + rs.getString(2));
					System.out.print( " --> OBJ --> " + rs.getString(3));
					System.out.print( " --> TEMP_MB --> " + rs.getInt(4) );
					System.out.print( "  --> PGA_MB -->  " +rs.getInt(5) );
					System.out.println(" --> REDO_MB --> " +rs.getInt(6));
					
				}
				Thread.currentThread().sleep(300);
				rs.close();
				i++;
			}
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
}
