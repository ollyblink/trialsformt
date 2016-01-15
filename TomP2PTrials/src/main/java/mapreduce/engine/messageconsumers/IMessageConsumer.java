package mapreduce.engine.messageconsumers;

import mapreduce.engine.executors.IExecutor;
import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.storage.IDHTConnectionProvider;

public interface IMessageConsumer
// extends Runnable
{

	public void handleCompletedTask(Job job, ExecutorTaskDomain outputDomain, JobProcedureDomain inputDomain);

	public void handleCompletedProcedure(Job job, JobProcedureDomain outputDomain, JobProcedureDomain inputDomain);

	public IMessageConsumer dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider);

	public IExecutor executor();

	public IMessageConsumer executor(IExecutor executor);

	// public void executeNext();
	// public IMessageConsumer canTake(boolean canTake);
	//
	// public boolean canTake();

	// void executeMessage();

}
