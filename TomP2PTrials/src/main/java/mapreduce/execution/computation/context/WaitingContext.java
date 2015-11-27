package mapreduce.execution.computation.context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.broadcasthandler.AbstractMessageConsumer;
import mapreduce.execution.jobtask.Task;

public class WaitingContext implements IContext {
	private static Logger logger = LoggerFactory.getLogger(AbstractMessageConsumer.class);

	private static final long SLEEP_TIME = 10;
	private Task task;

	@Override
	public void write(Object keyOut, Object valueOut) {
		try {
			logger.info(task.id()+" " +keyOut + " "+valueOut+" "+SLEEP_TIME+"ms sleeping time.");
			Thread.sleep(SLEEP_TIME);
		} catch (InterruptedException e) { 
			e.printStackTrace();
		}
	}

	@Override
	public WaitingContext task(Task task) {
		this.task = task;
		return this;
	}

	public static WaitingContext newWaitingContext() {
		return new WaitingContext();
	}

}
