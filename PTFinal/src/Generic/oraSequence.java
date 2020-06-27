package Generic;

import java.util.concurrent.locks.ReentrantLock;

public class oraSequence {
	static int value = 0;
	static ReentrantLock a = new ReentrantLock();
	public  static int nextVal() {
		try {
			a.lock();
			value = value + 1;
			return value;
		}
		catch(Exception E) {
			
		}
		finally {
			a.unlock();
			
		}
		
		value++;
		return value;
	}
	public synchronized static int getval() {
		return value;
	}
	public synchronized static void setVal(int val) {
		value = val + 100000;
	}
	public synchronized static void reset() {
		value  = 0;
	}
}
