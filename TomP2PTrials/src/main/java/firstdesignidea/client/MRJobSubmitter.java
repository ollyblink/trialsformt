package firstdesignidea.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import firstdesignidea.execution.broadcasthandler.broadcastmessages.IBCMessage;
import firstdesignidea.execution.datasplitting.ITaskSplitter;
import firstdesignidea.execution.jobtask.Job;
import firstdesignidea.storage.DHTConnectionProvider;
import firstdesignidea.storage.IBroadcastListener;

public class MRJobSubmitter implements IBroadcastListener {
	private static Logger logger = LoggerFactory.getLogger(MRJobSubmitter.class);
	private DHTConnectionProvider dhtConnectionProvider;
	private ITaskSplitter taskSplitter;

	private MRJobSubmitter() {
	}

	public static MRJobSubmitter newMapReduceJobSubmitter() {
		return new MRJobSubmitter();
	}

	/**
	 * 
	 * @param job
	 * @return
	 */
	public void submit(final Job job) {
		dhtConnectionProvider.connect();
		System.out.println("connect");
		taskSplitter.splitAndEmit(job, dhtConnectionProvider);
		System.out.println("split");
//		dhtConnectionProvider.addJob(job);
		System.out.println("Add job");
	}

	public MRJobSubmitter dhtConnectionProvider(DHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
		dhtConnectionProvider.broadcastDistributor().broadcastListener(this);
		return this;
	}

	public DHTConnectionProvider dhtConnectionProvider() {
		return this.dhtConnectionProvider;
	}

	public MRJobSubmitter taskSplitter(ITaskSplitter taskSplitter) {
		this.taskSplitter = taskSplitter;
		return this;
	}

	@Override
	public void inform(IBCMessage bcMessage) {
		System.out.println("MRJobSubmitter received BC message with status: " +bcMessage.status());
	}
 

}
