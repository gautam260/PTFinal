package Generic;

public class oraSequence {
	static volatile int value = 0;
	public synchronized static int nextVal() {
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
