package mapreduce.engine.messageConsumer;

import java.util.TreeMap;
import java.util.concurrent.PriorityBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.broadcasting.IBCMessage;
import mapreduce.execution.job.Job;

/**
 * <code>MessageConsumer</code> stores incoming <code>IBCMessage</code> on a queue for future processing
 * 
 * @author ozihler
 *
 */
public abstract class AbstractMessageConsumer implements IMessageConsumer {

	protected static Logger logger = LoggerFactory.getLogger(AbstractMessageConsumer.class);

	protected TreeMap<Job, PriorityBlockingQueue<IBCMessage>> jobs;

	private boolean canTake;

	protected AbstractMessageConsumer() {
		this.jobs = new TreeMap<>();
	}

	public PriorityBlockingQueue<IBCMessage> queueFor(Job job) {
		return jobs.get(job);
	}

	public TreeMap<Job, PriorityBlockingQueue<IBCMessage>> jobs() {
		return jobs;
	}

	@Override
	public void run() {

		while (jobs.isEmpty()) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		try {
			while (canTake()) {
				logger.info("Before Take message");
				IBCMessage nextMessage = jobs.get(jobs.firstKey()).take();
				logger.info("Execute next message: " + nextMessage);
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

	protected Job getJob(String jobId) {
		for (Job job : jobs.keySet()) {
			if (job.id().equals(jobId)) {
				return job;
			}
		}
		return null;
	}
}