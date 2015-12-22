package mapreduce.execution.computation.context;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.computation.IMapReduceProcedure;
import mapreduce.execution.task.Task;
import mapreduce.manager.broadcasthandler.broadcastmessageconsumer.AbstractMessageConsumer;
import net.tomp2p.peers.Number160;

public class WaitingContext implements IContext {
	private static final int DEFAULT_WAITING_TIME = 1;

	private static Logger logger = LoggerFactory.getLogger(AbstractMessageConsumer.class);

	private Task task;

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

	@Override
	public WaitingContext task(Task task) {
		this.task = task;
		return this;
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

	@Override
	public Number160 resultHash() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IContext combiner(IMapReduceProcedure combiner) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IMapReduceProcedure combiner() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<Object> keys() {
		// TODO Auto-generated method stub
		return null;
	}

}
