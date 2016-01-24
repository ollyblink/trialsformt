package mapreduce.engine.broadcasting.broadcasthandlers.timeout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.broadcasting.broadcasthandlers.AbstractMapReduceBroadcastHandler;
import mapreduce.engine.broadcasting.broadcasthandlers.JobCalculationBroadcastHandler;
import mapreduce.engine.broadcasting.broadcasthandlers.JobSubmissionBroadcastHandler;
import mapreduce.engine.broadcasting.messages.IBCMessage;
import mapreduce.execution.jobs.Job;

public abstract class AbstractTimeout implements Runnable {
	private static Logger logger = LoggerFactory.getLogger(AbstractTimeout.class);

	protected AbstractMapReduceBroadcastHandler broadcastHandler;
	protected Job job;
	protected volatile long retrievalTimestamp;
	protected long timeToLive;
	protected IBCMessage bcMessage;

	private long sleepingTime;

	public AbstractTimeout(AbstractMapReduceBroadcastHandler broadcastHandler, Job job, long currentTimestamp, IBCMessage bcMessage,
			long timeToLive) {
		this.broadcastHandler = broadcastHandler;
		this.job = job;
		this.timeToLive = timeToLive;
		retrievalTimestamp(currentTimestamp, bcMessage);

	}

	public AbstractTimeout retrievalTimestamp(long retrievalTimestamp, IBCMessage bcMessage) {
		logger.info("AbstractTimeout: updated timeout for job " + job);
		this.retrievalTimestamp = retrievalTimestamp;
		this.bcMessage = bcMessage;
		return this;
	}

	protected void sleep() {
		long diff = 0;
		while ((diff = (System.currentTimeMillis() - retrievalTimestamp)) < timeToLive) {
			this.sleepingTime = (timeToLive - diff);
			logger.info("Timeout: sleeping for " + sleepingTime + " ms");
			try {
				Thread.sleep(timeToLive);
			} catch (InterruptedException e) {
				logger.warn("Exception caught", e);
			}
		}
		this.sleepingTime = (timeToLive - diff);
	}

	public static AbstractTimeout create(AbstractMapReduceBroadcastHandler broadcastHandler, Job job, long retrievalTimestamp, IBCMessage bcMessage,
			long timeToLive) {
		return (broadcastHandler instanceof JobSubmissionBroadcastHandler
				? new JobSubmissionTimeout((JobSubmissionBroadcastHandler) broadcastHandler, job, retrievalTimestamp, bcMessage, timeToLive)
				: new JobCalculationTimeout((JobCalculationBroadcastHandler) broadcastHandler, job, retrievalTimestamp, bcMessage, timeToLive));
	}

};