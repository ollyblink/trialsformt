package mapreduce.engine.messageconsumers;

import mapreduce.engine.executors.IExecutor;
import mapreduce.engine.executors.JobCalculationExecutor;
import mapreduce.execution.ExecutorTaskDomain;
import mapreduce.execution.JobProcedureDomain;
import mapreduce.execution.job.Job;
import mapreduce.storage.IDHTConnectionProvider;

public class AbstractMessageConsumer implements IMessageConsumer {

	protected IExecutor executor;
	protected IDHTConnectionProvider dhtConnectionProvider;

	@Override
	public void handleCompletedTask(Job job, ExecutorTaskDomain outputDomain, JobProcedureDomain inputDomain) {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleCompletedProcedure(Job job, JobProcedureDomain outputDomain, JobProcedureDomain inputDomain) {
		// TODO Auto-generated method stub

	}

	@Override
	public IExecutor executor() {
		return this.executor;
	}

	@Override
	public IMessageConsumer dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
		return this;
	}

	@Override
	public IMessageConsumer executor(IExecutor executor) {
		this.executor = executor;
		return this;
	}

}
