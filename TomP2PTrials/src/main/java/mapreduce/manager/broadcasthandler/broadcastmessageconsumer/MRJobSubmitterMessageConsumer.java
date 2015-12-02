package mapreduce.manager.broadcasthandler.broadcastmessageconsumer;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import mapreduce.execution.jobtask.Job;
import mapreduce.execution.jobtask.Task;
import mapreduce.manager.broadcasthandler.broadcastmessages.BCStatusType;
import mapreduce.manager.broadcasthandler.broadcastmessages.IBCMessage;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.PeerAddress;

public class MRJobSubmitterMessageConsumer extends AbstractMessageConsumer {
	private String jobSubmitterID;

	private MRJobSubmitterMessageConsumer(String jobSubmitterID, BlockingQueue<IBCMessage> bcMessages, BlockingQueue<Job> jobs) {
		super(bcMessages, jobs);
		this.jobSubmitterID = jobSubmitterID;
	}

	public static MRJobSubmitterMessageConsumer newInstance(String jobSubmitterID, BlockingQueue<Job> jobs) {
		return new MRJobSubmitterMessageConsumer(jobSubmitterID, new PriorityBlockingQueue<IBCMessage>(), jobs);
	}

	@Override
	public void handleFinishedJob(String jobId, String jobSubmitterId) {
		if (this.jobSubmitterID.equals(jobSubmitterId)) {
			System.err.println("Finished job " + jobId);
		}
	}

	@Override
	public MRJobSubmitterMessageConsumer canTake(boolean canTake) {
		return (MRJobSubmitterMessageConsumer) super.canTake(canTake);
	}

	@Override
	protected void handleBCMessage(IBCMessage nextMessage) {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleFinishedAllTasks(String jobId, Collection<Task> tasks, PeerAddress sender) {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleTaskExecutionStatusUpdate(String jobId, String taskId, Tuple<PeerAddress, BCStatusType> toUpdate) {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleFinishedTaskComparion(String jobId, String taskId, Tuple<PeerAddress, Integer> finalDataLocation) {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleFinishedAllTaskComparisons(String jobId, Collection<Task> tasks, PeerAddress sender) {
		// TODO Auto-generated method stub

	}

}
