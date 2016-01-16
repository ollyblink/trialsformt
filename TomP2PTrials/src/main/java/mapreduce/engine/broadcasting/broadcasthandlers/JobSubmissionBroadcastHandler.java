package mapreduce.engine.broadcasting.broadcasthandlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.broadcasting.messages.BCMessageStatus;
import mapreduce.engine.broadcasting.messages.IBCMessage;
import mapreduce.execution.jobs.Job;

public class JobSubmissionBroadcastHandler extends AbstractMapReduceBroadcastHandler {

	private static Logger logger = LoggerFactory.getLogger(JobSubmissionBroadcastHandler.class);

	@Override
	public void evaluateReceivedMessage(IBCMessage bcMessage) {
		String jobId = bcMessage.inputDomain().jobId();
		String jobSubmitter = jobId.substring(jobId.lastIndexOf("("), jobId.lastIndexOf(")") + 1);

		// Only receive messages for jobs that have been added by this submitter
		if (messageConsumer.executor().id() != null && jobSubmitter.equals(messageConsumer.executor().id())) {
			processMessage(bcMessage, getJob(jobId));
		}

	}

	@Override
	public void processMessage(IBCMessage bcMessage, Job job) {
		logger.info(" job: " + job);
		if (job != null) {
			if (bcMessage.status() == BCMessageStatus.COMPLETED_PROCEDURE) {
				if (job.isFinished()) {// This is the only thing we actually care about... is it finished and can the data be retrieved?
					abortJobExecution(job);
					bcMessage.execute(job, messageConsumer);
					return;
				}
			}
			// Simply update the timer, nothing else to do...
			updateTimestamp(job, bcMessage);
		}
	}

	/**
	 *
	 * 
	 * @param nrOfConcurrentlyExecutedBCMessages
	 *            number of threads for this thread pool: how many bc messages may be executed at the same time?
	 * @return
	 */
	public static JobSubmissionBroadcastHandler create(int nrOfConcurrentlyExecutedBCMessages) {
		return new JobSubmissionBroadcastHandler(nrOfConcurrentlyExecutedBCMessages);
	}

	// Setter, Getter, Creator, Constructor follow below..
	private JobSubmissionBroadcastHandler(int nrOfConcurrentlyExecutedBCMessages) {
		super(nrOfConcurrentlyExecutedBCMessages);
	}

}
