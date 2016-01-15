package mapreduce.engine.messageconsumers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.executors.IExecutor;
import mapreduce.engine.executors.JobSubmissionExecutor;
import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.procedures.EndProcedure;
import mapreduce.storage.IDHTConnectionProvider;

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

	@Override
	public JobSubmissionMessageConsumer dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		return (JobSubmissionMessageConsumer) super.dhtConnectionProvider(dhtConnectionProvider);
	}

	@Override
	public JobSubmissionMessageConsumer executor(IExecutor executor) {
		return (JobSubmissionMessageConsumer) super.executor(executor);
	}
}
