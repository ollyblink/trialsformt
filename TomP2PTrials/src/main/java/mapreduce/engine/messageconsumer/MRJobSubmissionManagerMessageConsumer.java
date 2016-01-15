package mapreduce.engine.messageconsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.executor.MRJobExecutionManager;
import mapreduce.engine.executor.MRJobSubmissionManager;
import mapreduce.execution.ExecutorTaskDomain;
import mapreduce.execution.JobProcedureDomain;
import mapreduce.execution.job.Job;
import mapreduce.execution.procedures.EndProcedure;
import mapreduce.storage.IDHTConnectionProvider;

public class MRJobSubmissionManagerMessageConsumer implements IMessageConsumer {
	private static Logger logger = LoggerFactory.getLogger(MRJobExecutionManagerMessageConsumer.class);

	private MRJobSubmissionManager jobSubmissionManager;

	private MRJobSubmissionManagerMessageConsumer(MRJobSubmissionManager jobSubmissionManager) {
 
		this.jobSubmissionManager = jobSubmissionManager;
	}

	public static MRJobSubmissionManagerMessageConsumer create(MRJobSubmissionManager jobSubmissionManager) {
		return new MRJobSubmissionManagerMessageConsumer(jobSubmissionManager);
	}

	 

	@Override
	public void handleCompletedTask(Job job, ExecutorTaskDomain outputDomain, JobProcedureDomain inputDomain) {

	}

	@Override
	public void handleCompletedProcedure(Job job, JobProcedureDomain outputDomain, JobProcedureDomain inputDomain) {
		// Job job = getJob(inputDomain.jobId());
		if (job.jobSubmitterID().equals(jobSubmissionManager.id())) {
			if (outputDomain.procedureSimpleName().equals(EndProcedure.class.getSimpleName())) {
				logger.info("Job is finished. Final data location domain: " + outputDomain);
				jobSubmissionManager.finishedJob(outputDomain);
			}
		}
	}

	@Override
	public IMessageConsumer dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		 
		return null;
	}

	@Override
	public MRJobExecutionManager executor() {
		// TODO Auto-generated method stub
		return null;
	}
}
