package mapreduce.manager.broadcasthandler.broadcastmessageconsumer;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import mapreduce.execution.job.Job;
import mapreduce.manager.broadcasthandler.broadcastmessages.IBCMessage;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.storage.dhtmaintenance.IDHTDataCleaner;

public class MRJobCleanupManagerMessageConsumer extends AbstractMessageConsumer {

	private IDHTDataCleaner dhtDataCleaner;
	private IDHTConnectionProvider dhtConnectionProvider;

	protected MRJobCleanupManagerMessageConsumer(BlockingQueue<IBCMessage> bcMessages, List<Job> jobs, IDHTConnectionProvider dhtConnectionProvider) {
		super(bcMessages, jobs);
		this.dhtConnectionProvider = dhtConnectionProvider;
		this.dhtConnectionProvider.connect();
	}

	@Override
	public void handleFailedJob(Job job) {
//		dhtConnectionProvider.removeAll(job, task, dataForTask);
//		this.dhtDataCleaner.removeDataFromDHT();
	}

}
