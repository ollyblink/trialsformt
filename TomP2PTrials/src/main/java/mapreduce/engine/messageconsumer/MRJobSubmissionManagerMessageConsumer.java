package mapreduce.engine.messageconsumer;

import mapreduce.engine.executor.MRJobSubmissionManager;
import mapreduce.execution.ExecutorTaskDomain;
import mapreduce.execution.JobProcedureDomain;
import mapreduce.execution.job.Job;
import mapreduce.execution.procedures.EndProcedure;

public class MRJobSubmissionManagerMessageConsumer extends AbstractMessageConsumer {

	private MRJobSubmissionManager jobSubmissionManager;

	private MRJobSubmissionManagerMessageConsumer(MRJobSubmissionManager jobSubmissionManager) {
		super();
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
	public void handleCompletedTask(Job job, ExecutorTaskDomain outputDomain, JobProcedureDomain inputDomain) {

	}

	@Override
	public void handleCompletedProcedure(Job job, JobProcedureDomain outputDomain, JobProcedureDomain inputDomain) {
//		Job job = getJob(inputDomain.jobId());
		if (job.jobSubmitterID().equals(jobSubmissionManager.id())) {
			if (outputDomain.procedureSimpleName().equals(EndProcedure.class.getSimpleName())) {
				logger.info("Job is finished. Final data location domain: " + outputDomain);
				jobSubmissionManager.finishedJob(outputDomain);
			}
		}
	}
}
