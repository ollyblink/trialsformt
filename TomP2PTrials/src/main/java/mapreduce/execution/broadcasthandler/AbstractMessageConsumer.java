package mapreduce.execution.broadcasthandler;

import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.broadcasthandler.broadcastmessages.IBCMessage;
import mapreduce.execution.jobtask.Job;

/**
 * <code>MessageConsumer</code> stores incoming <code>IBCMessage</code> on a queue for future processing
 * 
 * @author ozihler
 *
 */
public abstract class AbstractMessageConsumer implements IMessageConsumer {

	protected static Logger logger = LoggerFactory.getLogger(AbstractMessageConsumer.class);

	protected BlockingQueue<IBCMessage> bcMessages;
	protected BlockingQueue<Job> jobs;

	private boolean canTake;

	protected AbstractMessageConsumer(BlockingQueue<IBCMessage> bcMessages, BlockingQueue<Job> jobs) {
		this.bcMessages = bcMessages;
		this.jobs = jobs;

	}

	@Override
	public void run() {
		try {
			while (canTake()) {
				final IBCMessage nextMessage = bcMessages.take();
				nextMessage.execute(this);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public AbstractMessageConsumer canTake(boolean canTake) {
		this.canTake = canTake;
		return this;
	}

	@Override
	public boolean canTake() {
		return this.canTake;
	}

	@Override
	public BlockingQueue<IBCMessage> queue() {
		return this.bcMessages;
	}

	@Override
	public BlockingQueue<Job> jobs() {
		return jobs;
	}
}