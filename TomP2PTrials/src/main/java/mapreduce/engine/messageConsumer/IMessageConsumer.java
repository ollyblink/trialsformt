package mapreduce.engine.messageConsumer;

import mapreduce.execution.ExecutorTaskDomain;
import mapreduce.execution.JobProcedureDomain;

public interface IMessageConsumer extends Runnable {

	public void handleCompletedTask(ExecutorTaskDomain outputDomain, JobProcedureDomain inputDomain);

	public void handleCompletedProcedure(JobProcedureDomain outputDomain, JobProcedureDomain inputDomain);

	public IMessageConsumer canTake(boolean canTake);

	public boolean canTake();

}
