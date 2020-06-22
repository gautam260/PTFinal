package FullScan;

import Generic.DBConnection;

public class ChainedRows {

	
	
	
	
	
	void createTable() {
		try {
			Connection oraCon = DBConnection.getOraConn();
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
	class InsertData implements Runnable{
		public void run() {
			try {
				
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
	}
}
