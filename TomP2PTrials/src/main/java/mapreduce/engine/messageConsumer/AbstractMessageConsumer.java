package mapreduce.engine.messageConsumer;

import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.concurrent.PriorityBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.broadcasting.IBCMessage;
import mapreduce.execution.job.Job;
import mapreduce.utils.SyncedCollectionProvider;

/**
 * <code>MessageConsumer</code> stores incoming <code>IBCMessage</code> on a queue for future processing
 * 
 * @author ozihler
 *
 */
public abstract class AbstractMessageConsumer implements IMessageConsumer {

	protected static Logger logger = LoggerFactory.getLogger(AbstractMessageConsumer.class);

	protected SortedMap<Job, PriorityBlockingQueue<IBCMessage>> jobs;

	private boolean canTake;

	protected AbstractMessageConsumer() {
		this.jobs = SyncedCollectionProvider.syncedTreeMap();
	}

	public PriorityBlockingQueue<IBCMessage> queueFor(Job job) {
		return jobs.get(job);
	}

	public SortedMap<Job, PriorityBlockingQueue<IBCMessage>> jobs() {
		return jobs;
	}

	@Override
	public void run() {
		logger.info(jobs + "");
		while (jobs.isEmpty()) {
			logger.info("Waiting for first job");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		while (canTake()) {
			try {
				Job job = jobs.firstKey();
				if (!job.isFinished()) {
					logger.info("Before Take message");
					IBCMessage nextMessage = jobs.get(job).take();  
					nextMessage.execute(job, this);
				}
			} catch (InterruptedException | NoSuchElementException e) {
				logger.info("Exception caught", e);
			}
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