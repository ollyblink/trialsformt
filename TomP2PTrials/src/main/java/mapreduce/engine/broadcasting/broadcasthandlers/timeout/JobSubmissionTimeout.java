package mapreduce.engine.broadcasting.broadcasthandlers.timeout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.broadcasting.broadcasthandlers.JobSubmissionBroadcastHandler;
import mapreduce.engine.broadcasting.messages.IBCMessage;
import mapreduce.engine.executors.JobSubmissionExecutor;
import mapreduce.execution.jobs.Job;

public class JobSubmissionTimeout extends AbstractTimeout {
	private static Logger logger = LoggerFactory.getLogger(JobSubmissionTimeout.class);

	public JobSubmissionTimeout(JobSubmissionBroadcastHandler broadcastHandler, Job job, long retrievalTimestamp, IBCMessage bcMessage,
			long timeToLive) {
		super(broadcastHandler, job, retrievalTimestamp, bcMessage, timeToLive);
	}

	@Override
	public void run() {
		sleep();
		synchronized (this.broadcastHandler) {
			logger.info(" Timeout: try resubmitting job " + job);
//			this.broadcastHandler.abortJobExecution(job);
			((JobSubmissionExecutor) broadcastHandler.messageConsumer().executor()).resubmitJobIfPossible(job);
		}
	}

}
