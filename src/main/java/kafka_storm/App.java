package kafka_storm;

import java.util.HashMap;

/**
 * Hello world!
 *
 */
public class App {
	
	public static void main(String[] args) {
		HashMap<String, String> hashMap = new HashMap<String, String>(16,8);
		for(int i=0;i<=1000;i++) {
			hashMap.put("i+"+i, "II"+i);
		}
//		System.out.println(hashMap.get("1"));
//		System.out.println(new BigDecimal(Math.pow(2, 30)).toPlainString());
//		System.out.println(Double.parseDouble("103218689310925"));
//		int index = 12;
//		int size = 4; // the size must greater than 4 --> size>=4;
//		double loadFactor = 0.75;
//		final int MAX = (int) Math.pow(2, size); // eg:2^4=16 2^5=32 ..
//
//		System.out.println(index & (MAX - 1));
//		double factor = loadFactor * MAX;
//		if (index++ >= factor) {
//			size++;
//			System.out.println(size);
//		}
	}
}

