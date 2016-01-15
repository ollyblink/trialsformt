package mapreduce.engine.messageconsumer;

import mapreduce.engine.executor.MRJobExecutionManager;
import mapreduce.execution.ExecutorTaskDomain;
import mapreduce.execution.JobProcedureDomain;
import mapreduce.execution.job.Job;
import mapreduce.storage.IDHTConnectionProvider;

public interface IMessageConsumer
//extends Runnable 
{

	public void handleCompletedTask(Job job, ExecutorTaskDomain outputDomain, JobProcedureDomain inputDomain);

	public void handleCompletedProcedure(Job job, JobProcedureDomain outputDomain, JobProcedureDomain inputDomain);

	public IMessageConsumer dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider);
	
	public MRJobExecutionManager executor();

//	public void executeNext();
//	public IMessageConsumer canTake(boolean canTake);
//
//	public boolean canTake();

//	void executeMessage();

}
