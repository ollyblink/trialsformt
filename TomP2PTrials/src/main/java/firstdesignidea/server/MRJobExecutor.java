package firstdesignidea.server;

import java.util.LinkedList;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import firstdesignidea.execution.broadcasthandler.broadcastmessages.DistributedTaskBCMessage;
import firstdesignidea.execution.broadcasthandler.broadcastmessages.IBCMessage;
import firstdesignidea.execution.jobtask.IJobReceiver;
import firstdesignidea.storage.DHTConnectionProvider;
import firstdesignidea.storage.IBroadcastListener;

public class MRJobExecutor implements IJobReceiver, IBroadcastListener {
	private static Logger logger = LoggerFactory.getLogger(MRJobExecutor.class);
	private DHTConnectionProvider dhtConnectionProvider;
	private Queue<IBCMessage> bcMessages;

	private MRJobExecutor() {
		this.bcMessages = new LinkedList<IBCMessage>();
	}

	public static MRJobExecutor newJobExecutor() {
		return new MRJobExecutor();
	}

	public MRJobExecutor dhtConnectionProvider(DHTConnectionProvider dhtConnectionProvider) {
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
		switch (bcMessage.status()) {
		case DISTRIBUTED_TASKS:
			DistributedTaskBCMessage dtbcMessage = (DistributedTaskBCMessage) bcMessage;
			System.out.println("MRjobexecutor");
			System.out.println("Received distributed task bc message:");
			System.out.println("jobid: " + dtbcMessage.jobId());
			System.out.println("task id: " + dtbcMessage.taskId());
			bcMessages.add(dtbcMessage);
			break;
		case EXECUTING_TASK:
			break;
		case FINISHED_TASK:
			break;
		case TASK_FAILED:
			break;
		case FINISHED_ALL_TASKS:
			break;
		default:
			break;
		}
	}

	@Override
	public void addJob(String taskId, String jobId) {
		// TODO Auto-generated method stub

	}

}
