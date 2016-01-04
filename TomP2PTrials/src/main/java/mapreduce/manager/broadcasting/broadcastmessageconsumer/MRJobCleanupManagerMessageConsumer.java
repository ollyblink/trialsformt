package mapreduce.manager.broadcasting.broadcastmessageconsumer;

import mapreduce.execution.ExecutorTaskDomain;
import mapreduce.execution.JobProcedureDomain;

public class MRJobCleanupManagerMessageConsumer extends AbstractMessageConsumer {

	@Override
	public void handleCompletedTask(ExecutorTaskDomain outputDomain, JobProcedureDomain inputDomain) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void handleCompletedProcedure(JobProcedureDomain outputDomain, JobProcedureDomain inputDomain) {
		// TODO Auto-generated method stub
		
	}
//
//	private IDHTDataCleaner dhtDataCleaner;
//	private IDHTConnectionProvider dhtConnectionProvider;
//
//	protected MRJobCleanupManagerMessageConsumer(BlockingQueue<IBCMessage> bcMessages, List<Job> jobs, IDHTConnectionProvider dhtConnectionProvider) {
//		super(bcMessages, jobs);
//		this.dhtConnectionProvider = dhtConnectionProvider;
//		this.dhtConnectionProvider.connect();
//	}
//
//	@Override
//	public void handleFailedJob(Job job) {
////		dhtConnectionProvider.removeAll(job, task, dataForTask);
////		this.dhtDataCleaner.removeDataFromDHT();
//	}

}
