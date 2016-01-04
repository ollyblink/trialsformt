package mapreduce.manager.broadcasting.broadcastmessageconsumer;

import mapreduce.execution.ExecutorTaskDomain;
import mapreduce.execution.JobProcedureDomain;
import mapreduce.execution.job.Job;
import mapreduce.execution.procedures.EndProcedure;
import mapreduce.manager.MRJobSubmissionManager;

public class MRJobSubmissionManagerMessageConsumer extends AbstractMessageConsumer {

	private MRJobSubmissionManager jobSubmissionManager;

	private MRJobSubmissionManagerMessageConsumer(MRJobSubmissionManager jobSubmissionManager) {
		this.jobSubmissionManager = jobSubmissionManager;
	}

	public static MRJobSubmissionManagerMessageConsumer create(MRJobSubmissionManager jobSubmissionManager) {
		return new MRJobSubmissionManagerMessageConsumer(jobSubmissionManager);
	}

	@Override
	public MRJobSubmissionManagerMessageConsumer canTake(boolean canTake) {
		return (MRJobSubmissionManagerMessageConsumer) super.canTake(canTake);
	}

	@Override
	public void handleCompletedTask(ExecutorTaskDomain outputDomain, JobProcedureDomain inputDomain) {
		// TODO Auto-generated method stub

	}

	private Job getJob(String jobId) {
		for (Job job : jobs.keySet()) {
			if (job.id().equals(jobId)) {
				return job;
			}
		}
		return null;
	}

	@Override
	public void handleCompletedProcedure(JobProcedureDomain outputDomain, JobProcedureDomain inputDomain) {
		Job job = getJob(inputDomain.jobId());
		if (job.jobSubmitterID().equals(jobSubmissionManager.id())) {
			if (outputDomain.procedureSimpleName().equals(EndProcedure.class.getSimpleName())) {
				logger.info("Job is finished. Final data location domain: " + outputDomain);
				jobSubmissionManager.finishedJob(outputDomain);
			}
		}
	}
}
