package mapreduce.engine.messageConsumer;

import mapreduce.execution.ExecutorTaskDomain;
import mapreduce.execution.JobProcedureDomain;
import mapreduce.execution.job.Job;

public interface IMessageConsumer extends Runnable {

	public void handleCompletedTask(Job job, ExecutorTaskDomain outputDomain, JobProcedureDomain inputDomain);

	public void handleCompletedProcedure(Job job, JobProcedureDomain outputDomain, JobProcedureDomain inputDomain);

	public IMessageConsumer canTake(boolean canTake);

	public boolean canTake();

}
