package mapreduce.manager.broadcasting.broadcastmessageconsumer;

import mapreduce.execution.procedures.ExecutorTaskDomain;
import mapreduce.execution.procedures.JobProcedureDomain;

public interface IMessageConsumer extends Runnable {

	public void handleCompletedTask(ExecutorTaskDomain outputDomain, JobProcedureDomain inputDomain);

	public void handleCompletedProcedure(JobProcedureDomain outputDomain, JobProcedureDomain inputDomain);

	public IMessageConsumer canTake(boolean canTake);

	public boolean canTake();

}
