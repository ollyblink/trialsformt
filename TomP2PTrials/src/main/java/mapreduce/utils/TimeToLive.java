package mapreduce.utils;

import java.util.List;

public enum TimeToLive {
	INSTANCE;
	private TimeToLive() {

	}

	public <T> boolean cancelOnTimeout(List<T> tasksToSchedule, long timeToSleep, long timeToLive) {
		long start = System.currentTimeMillis();
		while (tasksToSchedule.size() == 0) {
			// Needs to fetch data from dht... Wait until it has it or time ran out to retrieve it (timeToLive)
			try {
				Thread.sleep(timeToSleep);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			long tempEnd = System.currentTimeMillis();
			if (timeToLive <= (tempEnd - start)) { // ran out of time
				return false;
			}
		}
		return true;
	}
}
