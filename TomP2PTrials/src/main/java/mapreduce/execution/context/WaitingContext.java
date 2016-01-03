package mapreduce.execution.context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.manager.broadcasting.broadcastmessageconsumer.AbstractMessageConsumer;

public class WaitingContext extends AbstractBaseContext {
	private static final int DEFAULT_WAITING_TIME = 1;

	private static Logger logger = LoggerFactory.getLogger(AbstractMessageConsumer.class);

	private boolean shouldPrint;
	private long waitingTime;

	@Override
	public void write(Object keyOut, Object valueOut) {
		try {
			if (shouldPrint) {
				logger.info(task.id() + " " + keyOut + " " + valueOut + " " + waitingTime + "ms sleeping time.");
			}
			Thread.sleep(waitingTime);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public WaitingContext shouldPrint(boolean shouldPrint) {
		this.shouldPrint = shouldPrint;
		return this;
	}

	public static WaitingContext newInstance() {
		return new WaitingContext().shouldPrint(false).waitingTime(DEFAULT_WAITING_TIME);
	}

	public WaitingContext waitingTime(long waitingTime) {
		this.waitingTime = waitingTime;
		return this;
	}

}
