package mapreduce.engine.broadcasting.broadcasthandlers.timeout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.broadcasting.broadcasthandlers.AbstractMapReduceBroadcastHandler;
import mapreduce.engine.broadcasting.broadcasthandlers.JobSubmissionBroadcastHandler;
import mapreduce.engine.broadcasting.messages.IBCMessage;
import mapreduce.execution.jobs.Job;

public abstract class AbstractTimeout implements Runnable {
	private static Logger logger = LoggerFactory.getLogger(AbstractTimeout.class);

	protected AbstractMapReduceBroadcastHandler broadcastHandler;
	protected Job job;
	protected volatile long currentTimestamp;
	protected long timeToLive;
	protected long timeToSleep = 10000;
	protected IBCMessage bcMessage;

	public AbstractTimeout(AbstractMapReduceBroadcastHandler broadcastHandler, Job job, long currentTimestamp) {
		this.broadcastHandler = broadcastHandler;
		this.job = job;
		this.currentTimestamp = currentTimestamp;
		this.timeToLive = job.timeToLive();
	}

	public AbstractTimeout currentTimestamp(long currentTimestamp, IBCMessage bcMessage) {
		logger.info("AbstractTimeout: updated timeout for job " + job);
		this.currentTimestamp = currentTimestamp;
		this.bcMessage = bcMessage;
		return this;
	}

	protected void sleep() {
		while ((System.currentTimeMillis() - currentTimestamp) < timeToLive) {
			try {
				logger.info("for " + broadcastHandler.executorId() + "Timeout: sleeping for " + timeToSleep);
				Thread.sleep(timeToSleep);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static AbstractTimeout create(AbstractMapReduceBroadcastHandler broadcastHandler, Job job, long currentTimestamp) {
		return (broadcastHandler instanceof JobSubmissionBroadcastHandler ? new JobSubmissionTimeout(broadcastHandler, job, currentTimestamp)
				: new JobCalculationTimeout(broadcastHandler, job, currentTimestamp));
	}

};