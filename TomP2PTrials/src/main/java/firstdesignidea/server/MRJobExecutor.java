package firstdesignidea.server;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import firstdesignidea.execution.broadcasthandler.broadcastmessages.IBCMessage;
import firstdesignidea.execution.broadcasthandler.broadcastmessages.MessageConsumer;
import firstdesignidea.execution.jobtask.Job;
import firstdesignidea.execution.jobtask.JobStatus;
import firstdesignidea.execution.jobtask.Task;
import firstdesignidea.execution.scheduling.ITaskScheduler;
import firstdesignidea.storage.DHTConnectionProvider;
import firstdesignidea.storage.IBroadcastListener;

public class MRJobExecutor implements IBroadcastListener {
	private static Logger logger = LoggerFactory.getLogger(MRJobExecutor.class);
	private DHTConnectionProvider dhtConnectionProvider;
	private BlockingQueue<IBCMessage> bcMessages;
	private ITaskScheduler taskScheduler;
	private int maxNrOfFinishedPeers;

	private MRJobExecutor(DHTConnectionProvider dhtConnectionProvider) {
		this.bcMessages = new PriorityBlockingQueue<IBCMessage>();
		dhtConnectionProvider(dhtConnectionProvider);
		new Thread(new MessageConsumer(bcMessages, this));

	}

	public static MRJobExecutor newJobExecutor(DHTConnectionProvider dhtConnectionProvider) {
		return new MRJobExecutor(dhtConnectionProvider);
	}

	private MRJobExecutor dhtConnectionProvider(DHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
		this.dhtConnectionProvider.broadcastDistributor().broadcastListener(this);
		return this;
	}

	public DHTConnectionProvider dhtConnectionProvider() {
		return this.dhtConnectionProvider;
	}

	public void start() {
		dhtConnectionProvider.connect();
	}

	@Override
	public void inform(IBCMessage bcMessage) {
		bcMessages.add(bcMessage);
	}

	public MRJobExecutor maxNrOfFinishedPeers(int maxNrOfFinishedPeers) {
		this.maxNrOfFinishedPeers = maxNrOfFinishedPeers;
		return this;
	}

	public int maxNrOfFinishedPeers() {
		return this.maxNrOfFinishedPeers;
	}

	public void executeJob(Job job) {
		List<Task> tasks = this.taskScheduler.schedule(job);
		for (Task task : tasks) {
			if (task.numberOfPeersWithStatus(JobStatus.FINISHED_TASK) == this.maxNrOfFinishedPeers) {
				continue; // there are already enough peers that finished this task
			} else {
				this.dhtConnectionProvider.getDataForTask(task);
			}
		}
	}

}
