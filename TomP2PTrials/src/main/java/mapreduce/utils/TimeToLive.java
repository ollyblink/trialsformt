package mapreduce.utils;

import java.util.List;

import mapreduce.manager.conditions.ICondition;

public enum TimeToLive {
	INSTANCE;

	private static final long DEFAULT_SLEEPING_TIME = 10; // 10ms
	private static final long DEFAULT_TIME_TO_LIVE = 10000; // 10s

	private long sleepingTime = DEFAULT_SLEEPING_TIME;
	private long timeToLive = DEFAULT_TIME_TO_LIVE;

	private TimeToLive() {

	}

	public TimeToLive timeToLive(long timeToLive) {
		this.timeToLive = timeToLive;
		return this;
	}

	public TimeToLive sleepingTime(long sleepingTime) {
		this.sleepingTime = sleepingTime;
		return this;
	}

	public <T> boolean cancelOnTimeout(List<T> list, ICondition<List<T>> condition) {
		long start = System.currentTimeMillis();
		while (condition.metBy(list)) {
			// Wait until condition is met or time ran out to retrieve it (timeToLive)
			try {
				Thread.sleep(sleepingTime);
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
