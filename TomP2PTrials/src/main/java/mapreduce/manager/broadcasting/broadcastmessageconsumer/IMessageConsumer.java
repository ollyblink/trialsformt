package mapreduce.manager.broadcasting.broadcastmessageconsumer;

import mapreduce.execution.ExecutorTaskDomain;
import mapreduce.execution.JobProcedureDomain;

public interface IMessageConsumer extends Runnable {

	public void handleCompletedTask(ExecutorTaskDomain outputDomain, JobProcedureDomain inputDomain, int tasksSize);

	public void handleCompletedProcedure(JobProcedureDomain outputDomain, JobProcedureDomain inputDomain, int tasksSize);

	public IMessageConsumer canTake(boolean canTake);

	public boolean canTake();

}
