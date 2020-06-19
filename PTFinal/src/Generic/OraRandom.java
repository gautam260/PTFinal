package Generic;
import java.util.Random;

public class OraRandom {
	
	
	//Generates Random random distribution of data... value less than (int a)
	public	static int randomSkewInt(int a) {
		return Math.abs(Math.round((new Random().nextInt()/(new Random().nextInt()/4))%a));
	}
	
	//Generates Random uniform distribution of data... value less than (int a)
	public static int randomUniformInt(int a) {
		return Math.abs(new Random().nextInt()%a);
	}
	
	
	//Generates Random String Data of length A
	public static String randomString(int a) {
	    int leftLimit = 97;
	    int rightLimit = 122;
	    Random random = new Random();
	    StringBuilder buffer = new StringBuilder(a);
	    for (int i = 0; i < a; i++) {
	        int randomLimitedInt = leftLimit + (int) 
	          (random.nextFloat() * (rightLimit - leftLimit + 1));
	        buffer.append((char) randomLimitedInt);
	    }
	    return buffer.toString();
	}
	public static String randomBindString(int a) {
	    int leftLimit = 97;
	    int rightLimit = 122;
	    Random random = new Random();
	    StringBuilder buffer = new StringBuilder(a);
	    int temp = a/2;
	    for (int i = 0; i < temp; i++) {
	        int randomLimitedInt = leftLimit + (int) 
	          (random.nextFloat() * (rightLimit - leftLimit + 1));
	        buffer.append((char) randomLimitedInt);
	        buffer.append('%');
	    }
	    if (a%2==1) {
	    	   int randomLimitedInt = leftLimit + (int) (random.nextFloat() * (rightLimit - leftLimit + 1));
	    	buffer.append((char) randomLimitedInt);
	    }
	    return buffer.toString();
	}
}
//random Date: to_date(trunc(dbms_random.value(2458485,2458849)),'J')