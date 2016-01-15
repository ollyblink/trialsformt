package mapreduce.engine.messageconsumers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.executors.JobSubmissionExecutor;
import mapreduce.execution.ExecutorTaskDomain;
import mapreduce.execution.JobProcedureDomain;
import mapreduce.execution.job.Job;
import mapreduce.execution.procedures.EndProcedure;

public class JobSubmissionMessageConsumer extends AbstractMessageConsumer {
	private static Logger logger = LoggerFactory.getLogger(JobCalculationMessageConsumer.class);

	private JobSubmissionMessageConsumer() {

	}

	public static JobSubmissionMessageConsumer create() {
		return new JobSubmissionMessageConsumer();
	}

	@Override
	public void handleCompletedTask(Job job, ExecutorTaskDomain outputDomain, JobProcedureDomain inputDomain) {

	}

	@Override
	public void handleCompletedProcedure(Job job, JobProcedureDomain outputDomain, JobProcedureDomain inputDomain) {
		// Job job = getJob(inputDomain.jobId());
		if (job.jobSubmitterID().equals(executor.id())) {
			if (outputDomain.procedureSimpleName().equals(EndProcedure.class.getSimpleName())) {
				logger.info("Job is finished. Final data location domain: " + outputDomain);
				executor().finishedJob(outputDomain);
			}
		}
	}

	@Override
	public JobSubmissionExecutor executor() {
		return (JobSubmissionExecutor) super.executor();
	}
}
