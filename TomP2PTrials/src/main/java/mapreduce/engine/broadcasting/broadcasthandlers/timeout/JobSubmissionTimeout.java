package mapreduce.engine.broadcasting.broadcasthandlers.timeout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.broadcasting.broadcasthandlers.AbstractMapReduceBroadcastHandler;
import mapreduce.engine.executors.JobCalculationExecutor;
import mapreduce.engine.executors.JobSubmissionExecutor;
import mapreduce.execution.jobs.Job;

public class JobSubmissionTimeout extends AbstractTimeout {
	private static Logger logger = LoggerFactory.getLogger(JobSubmissionTimeout.class);

	public JobSubmissionTimeout(AbstractMapReduceBroadcastHandler broadcastHandler, Job job, long currentTimestamp) {
		super(broadcastHandler, job, currentTimestamp);
	}

	@Override
	public void run() {
		sleep();
		synchronized (this.broadcastHandler) {
			logger.info("for " + broadcastHandler.executorId() + " Timeout: try resubmitting job " + job);
			this.broadcastHandler.abortJobExecution(job);
			((JobSubmissionExecutor) broadcastHandler.messageConsumer().executor()).resubmitJobIfPossible(job);

		}
	}

}
