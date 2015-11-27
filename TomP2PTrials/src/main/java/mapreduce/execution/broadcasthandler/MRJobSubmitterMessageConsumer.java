package mapreduce.execution.broadcasthandler;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import mapreduce.execution.broadcasthandler.broadcastmessages.IBCMessage;
import mapreduce.execution.broadcasthandler.broadcastmessages.JobStatus;
import mapreduce.execution.jobtask.Job;
import mapreduce.execution.jobtask.Task;
import net.tomp2p.peers.PeerAddress;

public class MRJobSubmitterMessageConsumer extends AbstractMessageConsumer {
	private String jobSubmitterID;

	private MRJobSubmitterMessageConsumer(String jobSubmitterID, BlockingQueue<IBCMessage> bcMessages, BlockingQueue<Job> jobs) {
		super(bcMessages, jobs);
		this.jobSubmitterID = jobSubmitterID;
	}

	public static MRJobSubmitterMessageConsumer newMRJobSubmitterMessageConsumer(String jobSubmitterID, BlockingQueue<Job> jobs) {
		return new MRJobSubmitterMessageConsumer(jobSubmitterID,new PriorityBlockingQueue<IBCMessage>(), jobs);
	}

	@Override
	public void addJob(Job job) {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateTask(String jobId, String taskId, PeerAddress peerAddress, JobStatus currentStatus) {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleFinishedTasks(String jobId, Collection<Task> tasks) {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleFinishedJob(String jobId, String jobSubmitterId) {
		if(this.jobSubmitterID.equals(jobSubmitterId)){
			System.err.println("Finished job "+ jobId);
		}
	}
	
	@Override
	public MRJobSubmitterMessageConsumer canTake(boolean canTake) {
		return (MRJobSubmitterMessageConsumer) super.canTake(canTake);
	}
	
	
}
