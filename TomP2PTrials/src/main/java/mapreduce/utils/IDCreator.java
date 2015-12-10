package mapreduce.utils;

import java.util.Random;

public enum IDCreator {
	INSTANCE; 
	
	private final Random random = new Random();

	public String createTimeRandomID(final String name) {
		return name.toUpperCase() + "_" + System.currentTimeMillis() + "_" + random.nextLong();
	}
}
