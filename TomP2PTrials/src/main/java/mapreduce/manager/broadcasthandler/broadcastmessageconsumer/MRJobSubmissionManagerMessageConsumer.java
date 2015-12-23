package mapreduce.manager.broadcasthandler.broadcastmessageconsumer;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import mapreduce.execution.job.Job;
import mapreduce.manager.MRJobSubmissionManager;
import mapreduce.manager.broadcasthandler.broadcastmessages.IBCMessage;
import mapreduce.utils.DomainProvider;

public class MRJobSubmissionManagerMessageConsumer extends AbstractMessageConsumer {
	private String jobSubmitterID;

	private MRJobSubmissionManager jobSubmissionManager;

	private MRJobSubmissionManagerMessageConsumer(String jobSubmitterID, BlockingQueue<IBCMessage> bcMessages, List<Job> jobs) {
		super(bcMessages, jobs);
		this.jobSubmitterID = jobSubmitterID;
	}

	public static MRJobSubmissionManagerMessageConsumer newInstance(String jobSubmitterID, List<Job> jobs) {
		return new MRJobSubmissionManagerMessageConsumer(jobSubmitterID, new PriorityBlockingQueue<IBCMessage>(), jobs);
	}

	/**
	 * To resubmit the job if necessary
	 * 
	 * @param mrJobExecutor
	 * @return
	 */
	public MRJobSubmissionManagerMessageConsumer jobSubmissionManager(MRJobSubmissionManager jobSubmissionManager) {
		this.jobSubmissionManager = jobSubmissionManager;
		return this;
	}

	@Override
	public MRJobSubmissionManagerMessageConsumer canTake(boolean canTake) {
		return (MRJobSubmissionManagerMessageConsumer) super.canTake(canTake);
	}

	@Override
	public void handleFinishedJob(Job job) {
		if (this.jobSubmitterID.equals(job.jobSubmitterID())) {
			String jobProcedureDomain = DomainProvider.INSTANCE.jobProcedureDomain(job);
			logger.warn("handleFinishedJob()::1::Finished job " + DomainProvider.INSTANCE.jobProcedureDomain(job));
			this.jobSubmissionManager.finishedJob(jobProcedureDomain);
		}
	}

	@Override
	public void handleFailedJob(Job job) {
		if (this.jobSubmitterID.equals(job.jobSubmitterID())) {
			logger.warn("handleFailedJob()::1::Job failed:" + job.id());
			if (job.submissionCounter() < job.maxNrOfDHTActions()) {
				job.incrementSubmissionCounter();
				logger.warn("handleFailedJob()::2::Resubmitting job, " + job.submissionCounter() + ". time.");
				jobSubmissionManager.submit(job);
			} else {
				logger.warn("handleFailedJob()::3::Failed to submit job " + job.id() + ". ");
			}

		}
	}

	@Override
	public void handleReceivedJob(Job job) {
		if (this.jobSubmitterID.equals(job.jobSubmitterID())) {
			if (!jobs.contains(job)) {
				jobs.add(job);
				logger.warn("handleReceivedJob()::1::Received own job, added to jobs: " + job);
			} else {
				logger.warn("handleReceivedJob()::2::Received own job, discarded as its already received: " + job);
			}
		}
	}
}
