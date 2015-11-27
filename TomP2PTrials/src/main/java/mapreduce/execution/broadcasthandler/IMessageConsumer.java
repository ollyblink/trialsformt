package mapreduce.execution.broadcasthandler;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;

import mapreduce.execution.broadcasthandler.broadcastmessages.IBCMessage;
import mapreduce.execution.broadcasthandler.broadcastmessages.JobStatus;
import mapreduce.execution.jobtask.Job;
import mapreduce.execution.jobtask.Task;
import net.tomp2p.peers.PeerAddress;

public interface IMessageConsumer extends Runnable {
	public void addJob(Job job);
	public void updateTask(String jobId, String taskId, PeerAddress peerAddress, JobStatus currentStatus);
	public void handleFinishedTasks(String jobId, Collection<Task> tasks); 
	public void handleFinishedJob(String jobId, String jobSubmitterId);
	public BlockingQueue<IBCMessage> queue();
	public BlockingQueue<Job> jobs();
	public IMessageConsumer canTake(boolean canTake);
	public boolean canTake();
}
